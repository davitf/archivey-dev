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
    truncate_fraction: float = 0.25,
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
    num_corruptions: int = 5,
    corruption_byte_range: tuple[int, int] = (0, 255),
):
    """Copies the original_path to output_path and corrupts num_corruptions random bytes."""
    shutil.copyfile(original_path, output_path)
    with open(output_path, "rb+") as f:
        content = bytearray(f.read())
        size = len(content)

        if size == 0:  # Cannot corrupt an empty file
            return

        # If the file size is less than num_corruptions, reduce num_corruptions
        actual_num_corruptions = min(num_corruptions, size)

        # Avoid very first and very last few bytes (e.g., 4 bytes)
        # to ensure it's not just truncation or header damage.
        # Ensure the range is valid.
        min_safe_offset = 4
        max_safe_offset = size - 5  # -1 for 0-based index, -4 for the last 4 bytes

        if size <= min_safe_offset + 4:  # if file is too small for safe corruption zone
            # Corrupt any byte if file is too small for "safe" range
            corruption_indices = random.sample(range(size), actual_num_corruptions)
        else:
            corruption_indices = random.sample(
                range(min_safe_offset, max_safe_offset + 1), actual_num_corruptions
            )

        for pos in corruption_indices:
            random_byte = random.randint(
                corruption_byte_range[0], corruption_byte_range[1]
            )
            content[pos] = random_byte

        f.seek(0)
        f.write(content)
        # Ensure file size does not change if original operation was read-write binary
        # If original was 'r+b', then f.truncate() might be needed if content written is smaller.
        # However, bytearray modification and overwrite should maintain size.


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
            if archive_info.format_info.generation_method == "external":
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

            truncated_output_path = CORRUPTED_ARCHIVES_DIR / (
                archive_info.filename + ".truncated"
            )
            corrupted_output_path = CORRUPTED_ARCHIVES_DIR / (
                archive_info.filename + ".corrupted"
            )

            print(
                f"Generating truncated version for: {archive_info.filename} -> {truncated_output_path}"
            )
            truncate_archive(original_archive_path, truncated_output_path)

            print(
                f"Generating corrupted version for: {archive_info.filename} -> {corrupted_output_path}"
            )
            corrupt_archive(original_archive_path, corrupted_output_path)

    print("Corrupted archive generation complete.")


if __name__ == "__main__":
    main()
