import pytest

from archivey import open_archive
from archivey.config import ExtractionFilter
from archivey.filters import (
    FilterError,
    create_filter,
    fully_trusted,
    tar_filter,
)
from archivey.types import ArchiveMember
from tests.archivey.sample_archives import SANITIZE_ARCHIVES, SampleArchive


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_fully_trusted_filter(sample_archive: SampleArchive, sample_archive_path: str):
    """Test the fully_trusted filter allows everything."""

    with open_archive(sample_archive_path) as archive:
        members = list(archive.iter_members_with_io(filter=fully_trusted))

        # Should get all members without any filtering
        assert len(members) > 0

        # Check that problematic files are still present
        filenames = {m.filename for m, _ in members}
        expected_filenames = {f.name for f in sample_archive.contents.files}
        assert filenames == expected_filenames


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_tar_filter(sample_archive: SampleArchive, sample_archive_path: str):
    """Test the tar_filter raises errors on unsafe content."""

    with open_archive(sample_archive_path) as archive:
        with pytest.raises(
            FilterError,
            match="(Absolute path not allowed|Path outside archive root)",
        ):
            list(archive.iter_members_with_io(filter=tar_filter))


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_data_filter(sample_archive: SampleArchive, sample_archive_path: str):
    """Test the data_filter raises errors on unsafe content."""

    with open_archive(sample_archive_path) as archive:
        with pytest.raises(
            FilterError,
            match="(Absolute path not allowed|Path outside archive root)",
        ):
            list(archive.iter_members_with_io(filter=ExtractionFilter.DATA))


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_filter_with_raise_on_error_false(
    sample_archive: SampleArchive, sample_archive_path: str
):
    """Test filter that logs warnings instead of raising errors."""

    custom_filter = create_filter(
        for_data=False,
        sanitize_names=True,
        sanitize_link_targets=True,
        sanitize_permissions=True,
        raise_on_error=False,
    )

    with open_archive(sample_archive_path) as archive:
        # Should not raise an error, but should filter out problematic members
        members = list(archive.iter_members_with_io(filter=custom_filter))

        # Should get some members (the safe ones)
        assert len(members) > 0

        # Check that problematic files are filtered out
        filenames = {m.filename for m, _ in members}
        assert "/absfile.txt" not in filenames
        assert "../outside.txt" not in filenames


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_filter_without_name_sanitization(
    sample_archive: SampleArchive, sample_archive_path: str
):
    """Test filter that doesn't sanitize names."""

    custom_filter = create_filter(
        for_data=False,
        sanitize_names=False,
        sanitize_link_targets=True,
        sanitize_permissions=True,
        raise_on_error=True,
    )

    with open_archive(sample_archive_path) as archive:
        # Should still raise error due to link target sanitization
        with pytest.raises(FilterError, match="Symlink target outside archive root"):
            list(archive.iter_members_with_io(filter=custom_filter))


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_filter_without_link_target_sanitization(
    sample_archive: SampleArchive, sample_archive_path: str
):
    """Test filter that doesn't sanitize link targets."""

    custom_filter = create_filter(
        for_data=False,
        sanitize_names=True,
        sanitize_link_targets=False,
        sanitize_permissions=True,
        raise_on_error=True,
    )

    with open_archive(sample_archive_path) as archive:
        # Should still raise error due to name sanitization
        with pytest.raises(FilterError):
            list(archive.iter_members_with_io(filter=custom_filter))


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_filter_without_permission_sanitization(
    sample_archive: SampleArchive, sample_archive_path: str
):
    """Test filter that doesn't sanitize permissions."""

    custom_filter = create_filter(
        for_data=False,
        sanitize_names=True,
        sanitize_link_targets=True,
        sanitize_permissions=False,
        raise_on_error=True,
    )

    with open_archive(sample_archive_path) as archive:
        # Should still raise error due to name/link sanitization
        with pytest.raises(FilterError):
            list(archive.iter_members_with_io(filter=custom_filter))


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_data_filter_with_permission_changes(
    sample_archive: SampleArchive, sample_archive_path: str
):
    """Test data filter that changes permissions for files."""

    data_filter_custom = create_filter(
        for_data=True,
        sanitize_names=True,
        sanitize_link_targets=True,
        sanitize_permissions=True,
        raise_on_error=False,  # Don't raise to see permission changes
    )

    with open_archive(sample_archive_path) as archive:
        members = list(archive.iter_members_with_io(filter=data_filter_custom))

        # Check that executable files have permissions changed
        for member, _ in members:
            if member.is_file and "exec.sh" in member.filename:
                # The filter removes executable bits but keeps owner permissions as 0o644
                # Original mode is 493 (0o755), should become 420 (0o644)
                expected_mode = 0o644  # 420
                actual_mode = member.mode if member.mode is not None else "None"
                assert member.mode == expected_mode, (
                    f"Expected {oct(expected_mode)}, got {oct(actual_mode) if actual_mode != 'None' else 'None'}"
                )


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_filter_combinations(sample_archive: SampleArchive, sample_archive_path: str):
    # Test minimal filtering
    minimal_filter = create_filter(
        for_data=False,
        sanitize_names=False,
        sanitize_link_targets=False,
        sanitize_permissions=False,
        raise_on_error=False,
    )

    with open_archive(sample_archive_path) as archive:
        members = list(archive.iter_members_with_io(filter=minimal_filter))
        # Should get all members since no filtering is done
        assert len(members) > 0

        # Check that problematic files are still present
        filenames = [m.filename for m, _ in members]
        assert any("/absfile.txt" in f for f in filenames)
        assert any("../outside.txt" in f for f in filenames)


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_filter_error_messages(sample_archive: SampleArchive, sample_archive_path: str):
    """Test that filter errors have meaningful messages."""

    with open_archive(sample_archive_path) as archive:
        with pytest.raises(FilterError) as exc_info:
            list(archive.iter_members_with_io(filter=tar_filter))

        error_msg = str(exc_info.value)
        assert (
            "Absolute path not allowed" in error_msg
            or "Path outside archive root" in error_msg
        )


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_filter_with_dest_path(sample_archive: SampleArchive, sample_archive_path: str):
    """Test filter behavior with destination path specified."""

    custom_filter = create_filter(
        for_data=False,
        sanitize_names=True,
        sanitize_link_targets=True,
        sanitize_permissions=True,
        raise_on_error=True,
    )

    with open_archive(sample_archive_path) as archive:
        with pytest.raises(FilterError):
            list(archive.iter_members_with_io(filter=custom_filter))


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES[:1],
    ids=lambda x: x.filename,
)
def test_broken_filter(sample_archive: SampleArchive, sample_archive_path: str):
    """Test that a broken filter raises an error."""

    first_member: ArchiveMember | None = None

    def broken_filter(member: ArchiveMember) -> ArchiveMember | None:
        # A filter that caches and always returns the first member. The code should
        # notice that the returned member is different from the input member.
        nonlocal first_member
        if first_member is None:
            first_member = member

        return first_member.replace()  # Create a copy

    with open_archive(sample_archive_path) as archive:
        with pytest.raises(
            ValueError, match="Filter returned a member with a different internal ID"
        ):
            list(archive.iter_members_with_io(filter=broken_filter))
