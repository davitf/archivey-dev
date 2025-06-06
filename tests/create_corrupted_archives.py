import os
import pathlib
import random
import shutil

from tests.archivey.sample_archives import SAMPLE_ARCHIVES

CORRUPTED_ARCHIVES_DIR = pathlib.Path(__file__).parent / "test_corrupted_archives"
ORIGINAL_ARCHIVES_DIR_NAME = "test_archives"  # Relative to tests/
ORIGINAL_ARCHIVES_EXTERNAL_DIR_NAME = "test_archives_external"  # Relative to tests/


def truncate_archive(
    original_path: pathlib.Path,
    output_path: pathlib.Path,
    truncate_fraction: float = 0.5,
):
    """Copies the original_path to output_path and truncates the last truncate_fraction of its bytes."""
    shutil.copyfile(original_path, output_path)
    with open(output_path, "rb+") as f:
        size = os.path.getsize(output_path)
        truncate_at = int(size * (1 - truncate_fraction))
        f.truncate(truncate_at)


def corrupt_archive(
    original_path: pathlib.Path,
    output_path: pathlib.Path,
    corruption_type: str = "single",
):
    """Copies the original_path to output_path and corrupts it based on the corruption type.

    Args:
        original_path: Path to the original archive
        output_path: Path where the corrupted archive will be written
        corruption_type: Type of corruption to apply:
        position_fraction: Where to corrupt the file (0.0 to 1.0), used for "default" type
    """
    shutil.copyfile(original_path, output_path)
    with open(output_path, "rb+") as f:
        content = bytearray(f.read())
        size = len(content)

        if size == 0:  # Cannot corrupt an empty file
            return

        if corruption_type == "truncate":
            truncate_archive(original_path, output_path)
            return

        elif corruption_type == "single":
            position_fraction = 0.5
            num_bytes = 1
        elif corruption_type == "multiple":
            position_fraction = 0.5
            num_bytes = 128
        elif corruption_type == "zeroes":
            position_fraction = 0.5
            num_bytes = 128
        elif corruption_type == "ffs":
            position_fraction = 0.5
            num_bytes = 128
        else:
            raise ValueError(f"Invalid corruption type: {corruption_type}")

        corruption_position = int(size * position_fraction)
        f.seek(corruption_position)

        current_data = f.read(num_bytes)
        if corruption_type == "single":
            corrupted_data = bytes([current_data[0] ^ 0xFF])
        elif corruption_type == "multiple":
            r = random.Random(current_data)
            corrupted_data = r.randbytes(num_bytes)
        elif corruption_type == "zeroes":
            corrupted_data = bytes([0] * num_bytes)
        elif corruption_type == "ffs":
            corrupted_data = bytes([0xFF] * num_bytes)
        else:
            raise ValueError(f"Invalid corruption type: {corruption_type}")

        f.seek(corruption_position)
        f.write(corrupted_data)


def main():
    """Generates corrupted archive variants."""
    if CORRUPTED_ARCHIVES_DIR.exists():
        print(f"Cleaning up existing directory: {CORRUPTED_ARCHIVES_DIR}")
        for item in CORRUPTED_ARCHIVES_DIR.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                os.remove(item)
    else:
        CORRUPTED_ARCHIVES_DIR.mkdir(parents=True)
        print(f"Created directory: {CORRUPTED_ARCHIVES_DIR}")

    current_script_dir = pathlib.Path(__file__).parent

    for archive_info in SAMPLE_ARCHIVES:
        if archive_info.generate_corrupted_variants:
            # Determine original archive path
            if archive_info.creation_info.generation_method == "external":
                original_archive_path = (
                    current_script_dir
                    / ORIGINAL_ARCHIVES_EXTERNAL_DIR_NAME
                    / archive_info.filename
                )
            else:
                original_archive_path = (
                    current_script_dir
                    / ORIGINAL_ARCHIVES_DIR_NAME
                    / archive_info.filename
                )

            if not original_archive_path.exists():
                print(f"SKIPPING: Original archive not found: {original_archive_path}")
                continue

            truncated_output_path = (
                CORRUPTED_ARCHIVES_DIR / archive_info.get_archive_name("truncated")
            )

            print(
                f"Generating truncated version for: {archive_info.filename} -> {truncated_output_path}"
            )
            truncate_archive(original_archive_path, truncated_output_path)

            # Generate corrupted versions for each corruption type
            for corruption_type in ["header", "data", "checksum"]:
                corrupted_output_path = (
                    CORRUPTED_ARCHIVES_DIR
                    / archive_info.get_archive_name(f"corrupted_{corruption_type}")
                )

                print(
                    f"Generating corrupted version ({corruption_type}) for: {archive_info.filename} -> {corrupted_output_path}"
                )
                corrupt_archive(
                    original_archive_path,
                    corrupted_output_path,
                    corruption_type=corruption_type,
                )

    print("Corrupted archive generation complete.")


if __name__ == "__main__":
    main()
