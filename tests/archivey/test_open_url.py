import functools
import http.server
import os
import threading
from urllib.request import urlopen

import pytest

from archivey.core import open_archive
from archivey.exceptions import ArchiveStreamNotSeekableError
from archivey.types import ArchiveFormat
from tests.archivey.sample_archives import (
    ALTERNATIVE_CONFIG,
    BASIC_ARCHIVES,
    SINGLE_FILE_ARCHIVES,
    filter_archives,
)
from tests.archivey.test_open_nonseekable import EXPECTED_NON_SEEKABLE_FAILURES
from tests.archivey.testing_utils import skip_if_package_missing


def _serve_directory(directory: str):
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=directory
    )
    server = http.server.ThreadingHTTPServer(("localhost", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        BASIC_ARCHIVES + SINGLE_FILE_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format != ArchiveFormat.FOLDER,
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_open_archive_via_http(sample_archive, alternative_packages):
    config = ALTERNATIVE_CONFIG if alternative_packages else None
    skip_if_package_missing(sample_archive.creation_info.format, config)

    path = sample_archive.get_archive_path()
    server, thread = _serve_directory(directory=os.path.dirname(path))
    try:
        url = f"http://localhost:{server.server_address[1]}/{os.path.basename(path)}"
        with urlopen(url) as response:
            try:
                with open_archive(
                    response, streaming_only=True, config=config
                ) as archive:
                    has_member = False
                    for member, stream in archive.iter_members_with_streams():
                        has_member = True
                        if stream is not None:
                            stream.read()
                    assert has_member
            except (
                ArchiveStreamNotSeekableError
            ) as exc:  # pragma: no cover - env dependent
                key = (sample_archive.creation_info.format, alternative_packages)
                if key in EXPECTED_NON_SEEKABLE_FAILURES:
                    pytest.xfail(
                        f"Non-seekable {sample_archive.creation_info.format} are not supported with {alternative_packages=}: {exc}"
                    )
                else:
                    assert False, (
                        f"Expected format {key} to work with HTTP streams, but it failed with {exc!r}"
                    )
    finally:
        server.shutdown()
        thread.join()
