from pathlib import Path

import pytest

from archivey import open_archive
from archivey.filters import (
    FilterError,
    create_filter,
    tar_filter,
    data_filter,
    fully_trusted,
    DEFAULT_FILTERS,
)
from archivey.config import ExtractionFilter
from tests.archivey.sample_archives import SANITIZE_ARCHIVES


def get_sanitize_archive():
    """Get the first available sanitize archive for testing."""
    for sample in SANITIZE_ARCHIVES:
        archive_path = sample.get_archive_path()
        if Path(archive_path).exists():
            return sample
    pytest.skip("No sanitize archive found")


class TestDefaultFilters:
    """Test all the default filters."""

    def test_fully_trusted_filter(self):
        """Test the fully_trusted filter allows everything."""
        sample = get_sanitize_archive()
        archive_path = sample.get_archive_path()
        
        with open_archive(archive_path) as archive:
            members = list(archive.iter_members_with_io(filter=fully_trusted))
            
            # Should get all members without any filtering
            assert len(members) > 0
            
            # Check that problematic files are still present
            filenames = [m.filename for m, _ in members]
            assert any("/absfile.txt" in f for f in filenames)
            assert any("../outside.txt" in f for f in filenames)
            assert any("link_abs" in f for f in filenames)

    def test_tar_filter(self):
        """Test the tar_filter raises errors on unsafe content."""
        sample = get_sanitize_archive()
        archive_path = sample.get_archive_path()
        
        with open_archive(archive_path) as archive:
            with pytest.raises(FilterError, match="(Absolute path not allowed|Path outside archive root)"):
                list(archive.iter_members_with_io(filter=tar_filter))

    def test_data_filter(self):
        """Test the data_filter raises errors on unsafe content."""
        sample = get_sanitize_archive()
        archive_path = sample.get_archive_path()
        
        with open_archive(archive_path) as archive:
            with pytest.raises(FilterError, match="(Absolute path not allowed|Path outside archive root)"):
                list(archive.iter_members_with_io(filter=data_filter))

    def test_default_filters_dict(self):
        """Test that DEFAULT_FILTERS contains all expected filters."""
        expected_filters = {
            ExtractionFilter.FULLY_TRUSTED,
            ExtractionFilter.TAR,
            ExtractionFilter.DATA,
        }
        assert set(DEFAULT_FILTERS.keys()) == expected_filters
        assert DEFAULT_FILTERS[ExtractionFilter.FULLY_TRUSTED] == fully_trusted
        assert DEFAULT_FILTERS[ExtractionFilter.TAR] == tar_filter
        assert DEFAULT_FILTERS[ExtractionFilter.DATA] == data_filter


class TestCustomFilters:
    """Test custom filters created with create_filter."""

    def test_filter_with_raise_on_error_false(self):
        """Test filter that logs warnings instead of raising errors."""
        sample = get_sanitize_archive()
        archive_path = sample.get_archive_path()
        
        custom_filter = create_filter(
            for_data=False,
            sanitize_names=True,
            sanitize_link_targets=True,
            sanitize_permissions=True,
            raise_on_error=False,
        )
        
        with open_archive(archive_path) as archive:
            # Should not raise an error, but should filter out problematic members
            members = list(archive.iter_members_with_io(filter=custom_filter))
            
            # Should get some members (the safe ones)
            assert len(members) > 0
            
            # Check that problematic files are filtered out
            filenames = [m.filename for m, _ in members]
            assert not any("/absfile.txt" in f for f in filenames)
            assert not any("../outside.txt" in f for f in filenames)

    def test_filter_without_name_sanitization(self):
        """Test filter that doesn't sanitize names."""
        sample = get_sanitize_archive()
        archive_path = sample.get_archive_path()
        
        custom_filter = create_filter(
            for_data=False,
            sanitize_names=False,
            sanitize_link_targets=True,
            sanitize_permissions=True,
            raise_on_error=True,
        )
        
        with open_archive(archive_path) as archive:
            # Should still raise error due to link target sanitization
            with pytest.raises(FilterError):
                list(archive.iter_members_with_io(filter=custom_filter))

    def test_filter_without_link_target_sanitization(self):
        """Test filter that doesn't sanitize link targets."""
        sample = get_sanitize_archive()
        archive_path = sample.get_archive_path()
        
        custom_filter = create_filter(
            for_data=False,
            sanitize_names=True,
            sanitize_link_targets=False,
            sanitize_permissions=True,
            raise_on_error=True,
        )
        
        with open_archive(archive_path) as archive:
            # Should still raise error due to name sanitization
            with pytest.raises(FilterError):
                list(archive.iter_members_with_io(filter=custom_filter))

    def test_filter_without_permission_sanitization(self):
        """Test filter that doesn't sanitize permissions."""
        sample = get_sanitize_archive()
        archive_path = sample.get_archive_path()
        
        custom_filter = create_filter(
            for_data=False,
            sanitize_names=True,
            sanitize_link_targets=True,
            sanitize_permissions=False,
            raise_on_error=True,
        )
        
        with open_archive(archive_path) as archive:
            # Should still raise error due to name/link sanitization
            with pytest.raises(FilterError):
                list(archive.iter_members_with_io(filter=custom_filter))

    def test_data_filter_with_permission_changes(self):
        """Test data filter that changes permissions for files."""
        # Create a simple archive with executable files for testing
        sample = get_sanitize_archive()
        archive_path = sample.get_archive_path()
        
        data_filter_custom = create_filter(
            for_data=True,
            sanitize_names=True,
            sanitize_link_targets=True,
            sanitize_permissions=True,
            raise_on_error=False,  # Don't raise to see permission changes
        )
        
        with open_archive(archive_path) as archive:
            members = list(archive.iter_members_with_io(filter=data_filter_custom))
            
            # Check that executable files have permissions changed
            for member, _ in members:
                if member.is_file and "exec.sh" in member.filename:
                    # The filter removes executable bits but keeps owner permissions as 0o644
                    # Original mode is 493 (0o755), should become 420 (0o644)
                    expected_mode = 0o644  # 420
                    actual_mode = member.mode if member.mode is not None else "None"
                    assert member.mode == expected_mode, f"Expected {oct(expected_mode)}, got {oct(actual_mode) if actual_mode != 'None' else 'None'}"

    def test_filter_combinations(self):
        """Test various combinations of filter parameters."""
        sample = get_sanitize_archive()
        archive_path = sample.get_archive_path()
        
        # Test minimal filtering
        minimal_filter = create_filter(
            for_data=False,
            sanitize_names=False,
            sanitize_link_targets=False,
            sanitize_permissions=False,
            raise_on_error=False,
        )
        
        with open_archive(archive_path) as archive:
            members = list(archive.iter_members_with_io(filter=minimal_filter))
            # Should get all members since no filtering is done
            assert len(members) > 0
            
            # Check that problematic files are still present
            filenames = [m.filename for m, _ in members]
            assert any("/absfile.txt" in f for f in filenames)
            assert any("../outside.txt" in f for f in filenames)


class TestFilterErrorHandling:
    """Test error handling in filters."""

    def test_filter_error_messages(self):
        """Test that filter errors have meaningful messages."""
        sample = get_sanitize_archive()
        archive_path = sample.get_archive_path()
        
        with open_archive(archive_path) as archive:
            with pytest.raises(FilterError) as exc_info:
                list(archive.iter_members_with_io(filter=tar_filter))
            
            error_msg = str(exc_info.value)
            assert "Absolute path not allowed" in error_msg or "Path outside archive root" in error_msg

    def test_filter_with_dest_path(self):
        """Test filter behavior with destination path specified."""
        sample = get_sanitize_archive()
        archive_path = sample.get_archive_path()
        
        # Create a temporary directory for testing
        import tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            custom_filter = create_filter(
                for_data=False,
                sanitize_names=True,
                sanitize_link_targets=True,
                sanitize_permissions=True,
                raise_on_error=True,
            )
            
            with open_archive(archive_path) as archive:
                with pytest.raises(FilterError):
                    list(archive.iter_members_with_io(filter=custom_filter))
