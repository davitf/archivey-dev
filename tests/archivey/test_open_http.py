import contextlib
import functools
import http.server
import os
import threading
import urllib.request

import pytest

from archivey.core import open_archive
from archivey.exceptions import ArchiveStreamNotSeekableError
from archivey.types import ArchiveFormat
from tests.archivey.sample_archives import (
    BASIC_ARCHIVES,
    LARGE_ARCHIVES,
    SampleArchive,
    filter_archives,
)
from tests.archivey.testing_utils import skip_if_package_missing


@contextlib.contextmanager
def serve_file(path: str):
    directory = os.path.dirname(path)
    filename = os.path.basename(path)
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=directory)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}/{filename}"
    try:
        yield url
    finally:
        server.shutdown()
        thread.join()


EXPECTED_FAILURES = {
    ArchiveFormat.ZIP,
    ArchiveFormat.RAR,
    ArchiveFormat.SEVENZIP,
}


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        BASIC_ARCHIVES + LARGE_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format not in (ArchiveFormat.FOLDER,),
    ),
    ids=lambda a: a.filename,
)
def test_open_archive_http(sample_archive: SampleArchive, sample_archive_path: str):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    with serve_file(sample_archive_path) as url:
        with urllib.request.urlopen(url) as response:
            try:
                with open_archive(response, streaming_only=True) as archive:
                    members = []
                    for member, stream in archive.iter_members_with_streams():
                        members.append(member)
                        if stream is not None:
                            stream.read()
                    assert len(members) == len([f.name for f in sample_archive.contents.files])
            except ArchiveStreamNotSeekableError as exc:  # pragma: no cover - environment dependent
                fmt = sample_archive.creation_info.format
                if fmt in EXPECTED_FAILURES:
                    pytest.xfail(f"HTTP stream not supported for {fmt}: {exc}")
                else:
                    assert False, f"Expected format {fmt} to work over HTTP: {exc!r}"

