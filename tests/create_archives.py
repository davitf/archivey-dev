from datetime import datetime
import os
import subprocess
import tempfile
from typing import Generator
import zipfile
from archivey.types import MemberType, CompressionFormat

from tests.archivey.sample_archives import (
    SAMPLE_ARCHIVES,
    ArchiveInfo,
    FileInfo,
    GenerationMethod,
)


_COMPRESSION_METHOD_TO_ZIPFILE_VALUE = {
    "store": zipfile.ZIP_STORED,
    "deflate": zipfile.ZIP_DEFLATED,
    "bzip2": zipfile.ZIP_BZIP2,
    "lzma": zipfile.ZIP_LZMA,
}

DEFAULT_ZIP_COMPRESSION_METHOD = "store"


def write_files_to_dir(dir: str, files: list[FileInfo]):
    # Leave directories for last, so that their timestamps are not affected by the creation of the files inside them.
    for file in sorted(
        files,
        key=lambda x: [MemberType.FILE, MemberType.LINK, MemberType.DIR].index(x.type),
    ):
        full_path = os.path.join(dir, file.name)
        if file.type == MemberType.DIR:
            os.makedirs(full_path, exist_ok=True)
        elif file.type == MemberType.LINK:
            assert file.link_target is not None, "Link target is required"
            dir_path = os.path.dirname(full_path)
            os.makedirs(dir_path, exist_ok=True)
            os.symlink(
                file.link_target,
                full_path,
                target_is_directory=file.link_target_type == MemberType.DIR,
            )
        else:
            assert file.contents is not None, "File contents are required"
            dir_path = os.path.dirname(full_path)
            os.makedirs(dir_path, exist_ok=True)

            with open(full_path, "wb") as f:
                f.write(file.contents)

        os.utime(
            full_path,
            (datetime.now().timestamp(), file.mtime.timestamp()),
            follow_symlinks=False,
        )

    subprocess.run(["ls", "-l", dir])

    # write_file_to_dir(dir, file)


def group_files_by_password_and_compression_method(
    files: list[FileInfo],
) -> Generator[tuple[str | None, str | None, list[FileInfo]], None, None]:
    current_password: str | None = None
    current_compression_method: str | None = None
    current_files: list[FileInfo] = []
    for file in files:
        if (
            file.password != current_password
            or file.compression_method != current_compression_method
        ):
            if current_files:
                yield (current_password, current_compression_method, current_files)
            current_password = file.password
            current_compression_method = file.compression_method
            current_files = []
        current_files.append(file)

    if current_files:
        yield (current_password, current_compression_method, current_files)


def create_zip_archive_with_zipfile(
    archive_path: str, files: list[FileInfo], archive_comment: str | None = None
):
    """
    Create a zip archive using the zipfile module.

    This does not support symlinks.
    """
    for i, (password, _, group_files) in enumerate(
        group_files_by_password_and_compression_method(files)
    ):
        assert password is None, "zipfile does not support writing encrypted files"

        with zipfile.ZipFile(archive_path, "w" if i == 0 else "a") as zipf:
            if i == 0:
                zipf.comment = (archive_comment or "").encode("utf-8")

            for file in group_files:
                assert file.type != MemberType.LINK, (
                    "Links are not supported in zipfile"
                )
                if file.type == MemberType.DIR:
                    info = zipfile.ZipInfo(
                        file.name if file.name.endswith("/") else file.name + "/",
                        date_time=file.mtime.timetuple()[:6],
                    )
                    zipf.writestr(info, "")
                    continue

                assert file.contents is not None, "File contents are required"

                # zipf.setpassword((file.password or "").encode("utf-8"))
                info = zipfile.ZipInfo(file.name, date_time=file.mtime.timetuple()[:6])
                info.compress_type = _COMPRESSION_METHOD_TO_ZIPFILE_VALUE[
                    file.compression_method
                    if file.compression_method is not None
                    else DEFAULT_ZIP_COMPRESSION_METHOD
                ]
                info.comment = (file.comment or "").encode("utf-8")

                zipf.writestr(info, file.contents)


def create_zip_archive_with_infozip_command_line(
    archive_path: str, files: list[FileInfo], archive_comment: str | None = None
):
    """
    Create a zip archive using the zip command line tool.

    This supports symlinks, unlike the zipfile implementation. The files are written to
    the zip archive in the order of the files list.
    """

    abs_archive_path = os.path.abspath(archive_path)
    if os.path.exists(archive_path):
        os.remove(archive_path)

    with tempfile.TemporaryDirectory() as tempdir:
        write_files_to_dir(tempdir, files)

        # In order to apply the password to only the corresponding files, we need to use the --update option.
        for i, (password, compression_method, group_files) in enumerate(
            group_files_by_password_and_compression_method(files)
        ):
            command = ["zip"]
            if i > 0:
                command += ["--update"]
            command += ["--symlinks"]
            if password:
                command += ["-P", password]

            command += ["-Z", compression_method or DEFAULT_ZIP_COMPRESSION_METHOD]
            command += [abs_archive_path]

            # Pass the files to the command in the order they should be written to the archive.
            for file in group_files:
                command.append(file.name)

            # Run the command
            subprocess.run(command, check=True, cwd=tempdir)

        if archive_comment:
            command = ["zip", "-z", archive_path]
            subprocess.run(
                command,
                check=True,
                input=archive_comment.encode("utf-8"),
            )

        comment_file_names: list[str] = []
        comment_file_comments: list[str] = []
        for file in files:
            if file.comment:
                assert "\n" not in file.comment, "File comments cannot contain newlines"
                comment_file_names.append(file.name)
                comment_file_comments.append(file.comment)

        if comment_file_names:
            command = ["zip", "-c", archive_path] + comment_file_names
            print("Running command:", " ".join(command))
            subprocess.run(
                command,
                check=True,
                input="\n".join(comment_file_comments).encode("utf-8"),
            )


def create_tar_archive_with_command_line(
    archive_path: str,
    files: list[FileInfo],
    archive_comment: str | None = None,
    compression_format: CompressionFormat = CompressionFormat.TAR,
):
    """
    Create a tar archive using the tar command line tool.
    """
    if archive_comment:
        # TAR format does not support archive comments.
        # We could log a warning here if a logging mechanism is in place.
        pass

    abs_archive_path = os.path.abspath(archive_path)
    if os.path.exists(archive_path):
        os.remove(archive_path)

    with tempfile.TemporaryDirectory() as tempdir:
        write_files_to_dir(tempdir, files)

        command = ["tar"]
        command.append("-c")  # Create a new archive
        command.append("-f")  # Specify the archive file
        command.append(abs_archive_path)

        # Add compression flag based on the compression_format
        if compression_format == CompressionFormat.TAR_GZ:
            command.append("-z")  # gzip
        elif compression_format == CompressionFormat.TAR_BZ2:
            command.append("-j")  # bzip2
        elif compression_format == CompressionFormat.TAR_XZ:
            command.append("-J")  # xz
        elif compression_format != CompressionFormat.TAR:
            # This case should ideally not be reached if enums are used correctly
            raise ValueError(f"Unsupported tar compression format: {compression_format}")

        # Add file names to the command
        # These names must be relative to the temporary directory
        for file_info in files:
            command.append(file_info.name)
        
        try:
            subprocess.run(command, check=True, cwd=tempdir)
        except FileNotFoundError:
            # Handle case where tar command is not found
            # For testing purposes, we might want to skip tests or raise a specific exception
            print(f"Command 'tar' not found. Skipping TAR archive creation for {archive_path}")
            # Depending on requirements, could raise an error:
            # raise OSError("tar command not found, cannot create TAR archive.")
        except subprocess.CalledProcessError as e:
            print(f"Error creating TAR archive {archive_path}: {e}")
            # Optionally re-raise or handle as a test failure
            raise


GENERATION_METHODS_TO_GENERATOR = {
    GenerationMethod.ZIPFILE: create_zip_archive_with_zipfile,
    GenerationMethod.INFOZIP: create_zip_archive_with_infozip_command_line,
    GenerationMethod.TAR_COMMAND_LINE: create_tar_archive_with_command_line,
}


def create_archive(archive_info: ArchiveInfo, base_dir: str):
    full_path = os.path.join(base_dir, archive_info.filename)
    if archive_info.generation_method == GenerationMethod.EXTERNAL:
        # Check that the archive file exists
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"Archive file {full_path} does not exist")
        return

    generator = GENERATION_METHODS_TO_GENERATOR[archive_info.generation_method]
    if archive_info.generation_method == GenerationMethod.TAR_COMMAND_LINE:
        generator(
            full_path,
            archive_info.files,
            archive_info.archive_comment,
            archive_info.format,
        )
    else:
        generator(full_path, archive_info.files, archive_info.archive_comment)


def create_archives(archives: list[ArchiveInfo], base_dir: str):
    for archive_info in archives:
        create_archive(archive_info, base_dir)


if __name__ == "__main__":
    # Get the directory of the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    create_archives(SAMPLE_ARCHIVES, script_dir + "/test_archives")
