import subprocess
from pathlib import Path # Ensure imported
import pytest # Ensure imported

from tests.archivey.sample_archives import BASIC_ARCHIVES, SampleArchive, ENCRYPTED_ZIP_DEFLATE_PASSWORD_FOO # Added ENCRYPTED_ZIP_DEFLATE_PASSWORD_FOO
from tests.archivey.testing_utils import skip_if_package_missing
from archivey.types import ArchiveFormat # Ensure imported

# SAMPLE = BASIC_ARCHIVES[0] # Replaced by specific samples for new tests

# def _archive_path(tmpdir): # This helper might be less useful with specific samples
#     return SAMPLE.get_archive_path()

# Define specific samples for CLI tests
ZIP_SAMPLE_FOR_CLI = next(s for s in BASIC_ARCHIVES if s.filename == "basic_nonsolid__infozip.zip")
TARGZ_SAMPLE_FOR_CLI = next(s for s in BASIC_ARCHIVES if s.filename == "basic_solid__tarfile.tar.gz")
ENCRYPTED_SAMPLE_CLI = ENCRYPTED_ZIP_DEFLATE_PASSWORD_FOO
CORRECT_PASSWORD = "foo"


def test_cli_list(capsys):
    sample = BASIC_ARCHIVES[0] # Keep using the first sample for this existing test for now
    archive_path_str = sample.get_archive_path()
    skip_if_package_missing(sample.creation_info.format, None)
    result = subprocess.run(
        ["archivey", "--list", "--hide-progress", archive_path_str],
        text=True,
        capture_output=True, # Using capture_output
    )
    assert result.returncode == 0, f"CLI command failed: {result.stderr}"
    assert sample.contents.files[0].name.split("/")[0] in result.stdout


def test_cli_extract(tmp_path: Path): # Added Path type hint
    sample = BASIC_ARCHIVES[0] # Keep using the first sample for this existing test
    archive_path_str = sample.get_archive_path()
    skip_if_package_missing(sample.creation_info.format, None)
    dest = tmp_path / "out"
    result = subprocess.run(
        [
            "archivey",
            "--extract",
            "--dest",
            str(dest),
            "--hide-progress",
            archive_path_str,
        ],
        text=True,
        capture_output=True, # Using capture_output
    )
    assert result.returncode == 0, f"CLI command failed: {result.stderr}"
    expected = sample.contents.files[0].name.rstrip("/")
    assert (dest / expected).exists()


@pytest.mark.parametrize(
    "sample,explicit_format_val",
    [
        (ZIP_SAMPLE_FOR_CLI, ArchiveFormat.ZIP.value),
        (TARGZ_SAMPLE_FOR_CLI, ArchiveFormat.TAR_GZ.value),
    ]
)
def test_cli_list_explicit_format_correct(tmp_path: Path, sample: SampleArchive, explicit_format_val: str):
    archive_path = sample.get_archive_path() # Not using tmp_path to read existing files
    skip_if_package_missing(sample.creation_info.format, None)

    result = subprocess.run(
        ["archivey", "--list", "--hide-progress", "--format", explicit_format_val, archive_path],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, f"CLI command failed: {result.stderr}"
    assert sample.contents.files[0].name.split("/")[0] in result.stdout
    # Ensure no error messages are in stderr, allowing for debug/info messages
    assert "Error" not in result.stderr and "Traceback" not in result.stderr


def test_cli_list_explicit_format_incorrect(tmp_path: Path):
    sample = ZIP_SAMPLE_FOR_CLI
    archive_path = sample.get_archive_path()
    skip_if_package_missing(sample.creation_info.format, None) # sample.creation_info.format is ZIP

    # Try to open a ZIP file as TAR
    result = subprocess.run(
        ["archivey", "--list", "--hide-progress", "--format", ArchiveFormat.TAR.value, archive_path],
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0, "CLI command should have failed due to incorrect format"
    assert "Error opening archive with specified format tar" in result.stderr


def test_cli_list_explicit_format_invalid_string(tmp_path: Path):
    sample = ZIP_SAMPLE_FOR_CLI
    archive_path = sample.get_archive_path()
    # No need for skip_if_package_missing as this tests CLI parsing before format handling

    result = subprocess.run(
        ["archivey", "--list", "--hide-progress", "--format", "non_existent_format", archive_path],
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0, "CLI command should have failed due to invalid format string"
    # Error message comes from argparse for invalid choice if not caught by our custom check first
    # Based on current cli.py, it's "Error: Invalid format specified: non_existent_format"
    # For argparse error, it would be something like: "invalid choice: 'non_existent_format'"
    assert ("Invalid format specified: non_existent_format" in result.stderr or
            "invalid choice: 'non_existent_format'" in result.stderr)


def test_cli_list_explicit_format_folder(tmp_path: Path):
    # Create a dummy directory structure
    test_dir = tmp_path / "test_folder"
    test_dir.mkdir()
    (test_dir / "file_in_folder.txt").write_text("hello")

    result = subprocess.run(
        ["archivey", "--list", "--hide-progress", "--format", "folder", str(test_dir)],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, f"CLI command failed: {result.stderr}"
    assert "file_in_folder.txt" in result.stdout
    assert "Error" not in result.stderr and "Traceback" not in result.stderr


def test_cli_list_explicit_format_folder_non_dir(tmp_path: Path):
    non_dir_file = tmp_path / "not_a_folder.txt"
    non_dir_file.write_text("hello")

    result = subprocess.run(
        ["archivey", "--list", "--hide-progress", "--format", "folder", str(non_dir_file)],
        text=True,
        capture_output=True,
    )
    assert result.returncode != 0
    assert "Path must be a directory for format 'folder'" in result.stderr


def test_cli_list_encrypted_correct_password(tmp_path: Path):
    sample = ENCRYPTED_SAMPLE_CLI
    archive_path = sample.get_archive_path(tmp_path)

    skip_if_package_missing(sample.creation_info.format, None)

    result = subprocess.run(
        ["archivey", "--list", "--hide-progress", "--password", CORRECT_PASSWORD, archive_path],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, f"CLI command failed: {result.stderr}"
    assert sample.contents.files[0].name.split("/")[0] in result.stdout, f"Expected file not in stdout: {result.stdout}"
    assert "Error" not in result.stderr and "Traceback" not in result.stderr

def test_cli_list_encrypted_incorrect_password(tmp_path: Path):
    sample = ENCRYPTED_SAMPLE_CLI
    archive_path = sample.get_archive_path(tmp_path)
    skip_if_package_missing(sample.creation_info.format, None)

    result = subprocess.run(
        ["archivey", "--list", "--hide-progress", "--password", "wrongpassword", archive_path],
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0, "CLI command should have failed due to incorrect password"
    # Check for common error indicators in stderr
    assert "Error opening archive" in result.stderr or "password" in result.stderr.lower()


def test_cli_list_encrypted_no_password(tmp_path: Path):
    sample = ENCRYPTED_SAMPLE_CLI
    archive_path = sample.get_archive_path(tmp_path)
    skip_if_package_missing(sample.creation_info.format, None)

    result = subprocess.run(
        ["archivey", "--list", "--hide-progress", archive_path],
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0, "CLI command should have failed due to missing password"
    assert "Error opening archive" in result.stderr or "password" in result.stderr.lower()
