import functools
import http.server
import socketserver
import threading
from pathlib import Path
from urllib.request import urlopen

import pytest
from archivey.core import open_archive
from archivey.exceptions import ArchiveStreamNotSeekableError
from archivey.types import ArchiveFormat
from tests.archivey.sample_archives import (
    ALTERNATIVE_CONFIG,
    SAMPLE_ARCHIVES,
)
from tests.archivey.testing_utils import skip_if_package_missing

archives_by_format = {}
for a in SAMPLE_ARCHIVES:
    fmt = a.creation_info.format
    if fmt in (ArchiveFormat.FOLDER, ArchiveFormat.ISO):
        continue
    archives_by_format.setdefault(fmt, a)

EXPECTED_FAILURES = {
    (ArchiveFormat.GZIP, True),
    (ArchiveFormat.BZIP2, True),
    (ArchiveFormat.XZ, True),
    (ArchiveFormat.TAR_GZ, True),
    (ArchiveFormat.TAR_BZ2, True),
    (ArchiveFormat.TAR_XZ, True),
    (ArchiveFormat.ZIP, False),
    (ArchiveFormat.ZIP, True),
    (ArchiveFormat.RAR, False),
    (ArchiveFormat.RAR, True),
    (ArchiveFormat.SEVENZIP, False),
    (ArchiveFormat.SEVENZIP, True),
}


@pytest.mark.parametrize(
    "sample_archive",
    list(archives_by_format.values()),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize("alternative_packages", [False, True], ids=["defaultlibs", "altlibs"])
def test_open_from_url(sample_archive, sample_archive_path, alternative_packages):
    config = ALTERNATIVE_CONFIG if alternative_packages else None
    skip_if_package_missing(sample_archive.creation_info.format, config)

    path = Path(sample_archive_path)
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(path.parent))

    with socketserver.TCPServer(("127.0.0.1", 0), handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = f"http://127.0.0.1:{server.server_address[1]}/{path.name}"
        try:
            with urlopen(url) as resp:
                try:
                    with open_archive(resp, streaming_only=True, config=config) as archive:
                        has_member = False
                        for member, stream in archive.iter_members_with_io():
                            has_member = True
                            if stream is not None:
                                stream.read()
                        assert has_member
                except ArchiveStreamNotSeekableError as exc:
                    key = (sample_archive.creation_info.format, alternative_packages)
                    if key in EXPECTED_FAILURES:
                        pytest.xfail(
                            f"Non-seekable {sample_archive.creation_info.format} not supported over HTTP with {alternative_packages=}: {exc}"
                        )
                    else:
                        raise
        finally:
            server.shutdown()
            thread.join()
