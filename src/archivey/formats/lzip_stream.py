"""
Pure-stdlib lzip decompression using Python's lzma module.

lzip binary format (RFC-like structure, per lzip manual):
  Each file is a sequence of one or more *members*.  Members can be
  concatenated freely; readers must iterate until the compressed stream
  is exhausted.

  Per member:
    Header  (6 bytes):
      magic         4 bytes   b"LZIP"
      version       1 byte    must be 1
      coded_dict    1 byte    dict_size = 1 << (coded_dict & 0x1F)
                              valid range: exponent 12-29 (4 KiB - 512 MiB)

    LZMA1 data (variable length):
      Raw LZMA1 stream with an end-of-stream (EOS) marker.
      lzip always uses fixed LZMA parameters: lc=3, lp=0, pb=2.
      The stream does NOT include the standard 13-byte LZMA_ALONE header;
      we synthesise one so that Python's lzma.LZMADecompressor can handle it.

    Trailer (20 bytes):
      crc32         4 bytes LE   CRC-32 of the uncompressed member data
      data_size     8 bytes LE   byte count of the uncompressed member data
      member_size   8 bytes LE   byte count of the whole member (header + data + trailer)

Implementation notes:
  Python's lzma.LZMADecompressor(format=FORMAT_ALONE) expects the
  13-byte LZMA_ALONE header: props(1) + dict_size_LE(4) + uncompressed_size_LE(8).
  Because lzip omits this header, we build a synthetic one from the lzip header
  fields and feed it to the decompressor before any real data.  We set the
  uncompressed-size field to 0xFFFF…FF ("unknown") so the decompressor relies
  on the in-stream EOS marker instead.

  Once the LZMA stream ends, decompressor.eof becomes True and
  decompressor.unused_data holds any bytes that follow the EOS marker inside
  the chunk we fed — those bytes are the start of the 20-byte trailer.
"""

import lzma
import struct
import zlib

from archivey.exceptions import ArchiveCorruptedError, ArchiveEOFError

# lzip member header is always 6 bytes: magic(4) + version(1) + coded_dict(1)
_MAGIC = b"LZIP"
_HEADER_SIZE = 6
# lzip member trailer is always 20 bytes: CRC32(4) + data_size(8) + member_size(8)
_TRAILER_SIZE = 20

# lzip mandates lc=3, lp=0, pb=2 for all streams.
# LZMA_ALONE props byte encoding: (pb*5 + lp)*9 + lc = (2*5+0)*9+3 = 93 = 0x5D
_PROPS_BYTE = bytes([0x5D])

# Sentinel for "uncompressed size unknown" in the LZMA_ALONE header (8 × 0xFF).
# This tells lzma.LZMADecompressor to stop at the in-stream EOS marker.
_UNKNOWN_SIZE = b"\xff" * 8


class _LzipState:
    """
    Streaming state machine for multi-member lzip decompression.

    Call feed(chunk) repeatedly with successive compressed chunks; it returns
    the corresponding decompressed bytes.  When the underlying stream is
    exhausted, call flush() to finalise and detect truncation.

    The state machine cycles through three phases per member:

      NEED_HEADER  – accumulate 6 bytes, parse the member header, create a
                     fresh LZMADecompressor, transition to IN_MEMBER.

      IN_MEMBER    – feed bytes to the LZMADecompressor.  When its .eof flag
                     becomes True, any bytes after the LZMA EOS marker appear
                     in .unused_data; those start the trailer.  Transition to
                     NEED_TRAILER.

      NEED_TRAILER – accumulate 20 bytes, verify CRC-32 and data size against
                     what was actually decompressed, transition back to
                     NEED_HEADER for the next member (or cleanly finish if no
                     more data follows).
    """

    _NEED_HEADER = 0
    _IN_MEMBER = 1
    _NEED_TRAILER = 2

    def __init__(self) -> None:
        self._state = self._NEED_HEADER
        # Compressed bytes not yet assigned to a decompressor or trailer parser.
        self._buf = bytearray()
        self._dec: lzma.LZMADecompressor | None = None
        # Running CRC-32 and byte count for the current member's plaintext.
        self._crc = 0
        self._member_size = 0
        self._finished = False

    def feed(self, data: bytes) -> bytes:
        self._buf.extend(data)
        return self._process()

    def flush(self) -> bytes:
        """Called when the compressed stream is exhausted.

        Succeeds (sets finished=True, returns b"") only if we are cleanly
        between members with no buffered bytes.  Any other state means the
        stream was truncated.
        """
        if self._state == self._NEED_HEADER and not self._buf:
            self._finished = True
            return b""
        raise ArchiveEOFError("Lzip file is truncated")

    def is_finished(self) -> bool:
        return self._finished

    def _process(self) -> bytes:
        output = bytearray()
        while True:
            if self._state == self._NEED_HEADER:
                if len(self._buf) < _HEADER_SIZE:
                    break
                header = bytes(self._buf[:_HEADER_SIZE])
                del self._buf[:_HEADER_SIZE]
                self._start_member(header)
                self._state = self._IN_MEMBER

            elif self._state == self._IN_MEMBER:
                if not self._buf:
                    break
                chunk = bytes(self._buf)
                self._buf.clear()
                try:
                    assert self._dec is not None
                    plain = self._dec.decompress(chunk)
                except lzma.LZMAError as e:
                    raise ArchiveCorruptedError(
                        f"Error reading Lzip archive: {e}"
                    ) from e
                if plain:
                    self._crc = zlib.crc32(plain, self._crc)
                    self._member_size += len(plain)
                    output.extend(plain)
                if self._dec.eof:
                    # Bytes after the LZMA EOS marker belong to the trailer.
                    self._buf[0:0] = self._dec.unused_data
                    self._dec = None
                    self._state = self._NEED_TRAILER

            elif self._state == self._NEED_TRAILER:
                if len(self._buf) < _TRAILER_SIZE:
                    break
                trailer = bytes(self._buf[:_TRAILER_SIZE])
                del self._buf[:_TRAILER_SIZE]
                self._verify_trailer(trailer)
                self._state = self._NEED_HEADER

        return bytes(output)

    def _start_member(self, header: bytes) -> None:
        if header[:4] != _MAGIC:
            raise ArchiveCorruptedError(f"Invalid lzip magic: {header[:4]!r}")
        if header[4] != 1:
            raise ArchiveCorruptedError(f"Unsupported lzip version: {header[4]}")

        dict_size = 1 << (header[5] & 0x1F)

        # Build the 13-byte LZMA_ALONE header that Python's lzma module expects.
        # lzip stores only the dict size; lc/lp/pb are implicit (always 0x5D).
        lzma_alone_header = _PROPS_BYTE + struct.pack("<I", dict_size) + _UNKNOWN_SIZE
        self._dec = lzma.LZMADecompressor(format=lzma.FORMAT_ALONE)
        # Feeding the synthetic header initialises the decompressor state;
        # it produces no output because the header contains no compressed data.
        self._dec.decompress(lzma_alone_header)

        self._crc = 0
        self._member_size = 0

    def _verify_trailer(self, trailer: bytes) -> None:
        crc32_stored, data_size, _member_size = struct.unpack_from("<IQQ", trailer, 0)
        if (self._crc & 0xFFFFFFFF) != crc32_stored:
            raise ArchiveCorruptedError(
                f"Lzip CRC32 mismatch: stored {crc32_stored:#010x}, "
                f"computed {self._crc & 0xFFFFFFFF:#010x}"
            )
        if self._member_size != data_size:
            raise ArchiveCorruptedError(
                f"Lzip size mismatch: stored {data_size}, actual {self._member_size}"
            )
