from pathlib import Path

import pytest

from archivey import open_archive
from archivey.filters import FilterError, create_filter, tar_filter
from tests.archivey.sample_archives import SANITIZE_ARCHIVES


@pytest.mark.parametrize(
    "filter_func, expect_error",
    [
        (tar_filter, True),
        (
            create_filter(
                for_data=False,
                sanitize_names=True,
                sanitize_link_targets=True,
                sanitize_permissions=True,
                raise_on_error=True,
            ),
            False,
        ),
    ],
)
def test_iter_members_filter(filter_func, expect_error):
    sample = SANITIZE_ARCHIVES[0]
    archive_path = sample.get_archive_path()
    if not Path(archive_path).exists():
        pytest.skip(f"Archive {archive_path} missing")
    with open_archive(archive_path) as archive:
        if expect_error:
            with pytest.raises(FilterError):
                list(archive.iter_members_with_io(filter=filter_func))
        else:
            names = [
                m.filename for m, _ in archive.iter_members_with_io(filter=filter_func)
            ]
            assert "absfile.txt" in [Path(n).name for n in names]
