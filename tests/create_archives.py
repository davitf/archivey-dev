import logging
import os
import subprocess
import tempfile
from typing import Generator
import zipfile
import tarfile
import io
import stat
import py7zr
import argparse
import fnmatch
from archivey.types import MemberType, ArchiveFormat

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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
            (file.mtime.timestamp(), file.mtime.timestamp()),
            follow_symlinks=False,
        )

    # subprocess.run(["ls", "-l", dir])

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
                # zipfile module does not directly support symlinks in a way that preserves them as symlinks.
                # We are skipping symlink creation here for zipfile, but infozip handles it.
                # If mode is set for a symlink, it would apply to the target if zipfile wrote it as a regular file.
                # assert file.type != MemberType.LINK, (
                #     "Links are not supported in zipfile in a way that preserves them as symlinks."
                # )

                filename = file.name
                contents = file.contents

                if file.type == MemberType.DIR:
                    if not filename.endswith("/"):
                        filename += "/"
                    contents = b"" # Directories have no content

                elif file.type == MemberType.LINK:
                    # For zipfile, if we have to write a link, we write its target path as content.
                    # The external_attr will mark it as a link.
                    contents = (file.link_target or "").encode('utf-8')


                info = zipfile.ZipInfo(filename, date_time=file.mtime.timetuple()[:6])
                info.compress_type = _COMPRESSION_METHOD_TO_ZIPFILE_VALUE[
                    file.compression_method
                    if file.compression_method is not None
                    else DEFAULT_ZIP_COMPRESSION_METHOD
                ]
                info.comment = (file.comment or "").encode("utf-8")

                if file.mode is not None:
                    if file.type == MemberType.DIR:
                        info.external_attr = (stat.S_IFDIR | file.mode) << 16
                    elif file.type == MemberType.LINK:
                        info.external_attr = (stat.S_IFLNK | file.mode) << 16
                    else:  # MemberType.FILE or other treated as file
                        info.external_attr = (stat.S_IFREG | file.mode) << 16
                
                # Ensure directory names end with a slash for ZipInfo
                # This is now handled when setting filename above for MemberType.DIR
                # if file.type == MemberType.DIR and not info.filename.endswith('/'):
                #    info.filename += '/'
                
                if contents is None and file.type == MemberType.FILE:
                    assert False, f"File contents are required for {file.name}"


                zipf.writestr(info, contents if contents is not None else b"")


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
            command = ["zip", "-q"]
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
            logger.info("Running command: %s", " ".join(command))
            subprocess.run(
                command,
                check=True,
                input="\n".join(comment_file_comments).encode("utf-8"),
            )


def _create_tar_archive_with_tarfile( # Renamed and implemented with tarfile module
    archive_path: str,
    files: list[FileInfo],
    archive_comment: str | None = None,
    compression_format: ArchiveFormat = ArchiveFormat.TAR,
):
    """
    Create a tar archive using Python's tarfile module.
    Supports setting file modes and different compression formats.
    """
    assert archive_comment is None, "TAR format does not support archive comments"

    abs_archive_path = os.path.abspath(archive_path)
    if os.path.exists(abs_archive_path):
        os.remove(abs_archive_path)

    tar_mode = "w:"
    if compression_format == ArchiveFormat.TAR_GZ:
        tar_mode = "w:gz"
    elif compression_format == ArchiveFormat.TAR_BZ2:
        tar_mode = "w:bz2"
    elif compression_format == ArchiveFormat.TAR_XZ:
        tar_mode = "w:xz"
    elif compression_format == ArchiveFormat.TAR:
        tar_mode = "w" # plain tar
    else:
        raise ValueError(f"Unsupported tar compression format: {compression_format}")

    with tarfile.open(abs_archive_path, tar_mode) as tf:
        for sample_file in files:
            tarinfo = tarfile.TarInfo(name=sample_file.name)
            tarinfo.mtime = int(sample_file.mtime.timestamp())

            if sample_file.mode is not None:
                tarinfo.mode = sample_file.mode
            
            file_contents_bytes = sample_file.contents

            if sample_file.type == MemberType.DIR:
                tarinfo.type = tarfile.DIRTYPE
                if sample_file.mode is None:
                    tarinfo.mode = 0o755  # Default mode for directories
                tf.addfile(tarinfo)  # No fileobj for directories
            elif sample_file.type == MemberType.LINK:
                tarinfo.type = tarfile.SYMTYPE
                assert sample_file.link_target is not None, f"Link target required for {sample_file.name}"
                tarinfo.linkname = sample_file.link_target
                if sample_file.mode is None:
                    tarinfo.mode = 0o777  # Default mode for symlinks
                tf.addfile(tarinfo)  # No fileobj for symlinks
            else:  # MemberType.FILE
                assert file_contents_bytes is not None, f"Contents required for file {sample_file.name}"
                tarinfo.type = tarfile.REGTYPE
                tarinfo.size = len(file_contents_bytes)
                if sample_file.mode is None:
                    tarinfo.mode = 0o644  # Default mode for regular files
                tf.addfile(tarinfo, io.BytesIO(file_contents_bytes))


def create_gz_archive_with_command_line(
    archive_path: str, files: list[FileInfo], archive_comment: str | None = None
):
    assert len(files) == 1, "Gzip archives only support a single file."
    file_info = files[0]
    assert file_info.type == MemberType.FILE, "Only files are supported for gzip."
    assert archive_comment is None, "Gzip format does not support archive comments."

    abs_archive_path = os.path.abspath(archive_path)
    if os.path.exists(abs_archive_path):
        os.remove(abs_archive_path)

    with tempfile.TemporaryDirectory() as tempdir:
        temp_file_path = os.path.join(tempdir, file_info.name)
        with open(temp_file_path, "wb") as f:
            f.write(file_info.contents)
        os.utime(
            temp_file_path, (file_info.mtime.timestamp(), file_info.mtime.timestamp())
        )

        # gzip creates <temp_file_path>.gz and by default preserves mtime of source in the member.
        # --no-name prevents storing original filename/timestamp if different, but content mtime is kept.
        subprocess.run(["gzip", "--no-name", temp_file_path], check=True, cwd=tempdir)

        compressed_file_on_temp = temp_file_path + ".gz"
        os.rename(compressed_file_on_temp, abs_archive_path)

        # Explicitly set the mtime of the archive file itself
        os.utime(
            abs_archive_path, (file_info.mtime.timestamp(), file_info.mtime.timestamp())
        )


def create_bz2_archive_with_command_line(
    archive_path: str, files: list[FileInfo], archive_comment: str | None = None
):
    assert len(files) == 1, "Bzip2 archives only support a single file."
    file_info = files[0]
    assert file_info.type == MemberType.FILE, "Only files are supported for bzip2."
    assert archive_comment is None, "Bzip2 format does not support archive comments."

    abs_archive_path = os.path.abspath(archive_path)
    if os.path.exists(abs_archive_path):
        os.remove(abs_archive_path)

    with tempfile.TemporaryDirectory() as tempdir:
        temp_file_path = os.path.join(tempdir, file_info.name)
        with open(temp_file_path, "wb") as f:
            f.write(file_info.contents)
        os.utime(
            temp_file_path, (file_info.mtime.timestamp(), file_info.mtime.timestamp())
        )

        # bzip2 creates <temp_file_path>.bz2 and preserves mtime.
        subprocess.run(["bzip2", temp_file_path], check=True, cwd=tempdir)

        compressed_file_on_temp = temp_file_path + ".bz2"
        os.rename(compressed_file_on_temp, abs_archive_path)

        # Explicitly set the mtime of the archive file itself
        os.utime(
            abs_archive_path, (file_info.mtime.timestamp(), file_info.mtime.timestamp())
        )


def create_xz_archive_with_command_line(
    archive_path: str, files: list[FileInfo], archive_comment: str | None = None
):
    assert len(files) == 1, "XZ archives only support a single file."
    file_info = files[0]
    assert file_info.type == MemberType.FILE, "Only files are supported for xz."
    assert archive_comment is None, "XZ format does not support archive comments."

    abs_archive_path = os.path.abspath(archive_path)
    if os.path.exists(abs_archive_path):
        os.remove(abs_archive_path)

    with tempfile.TemporaryDirectory() as tempdir:
        temp_file_path = os.path.join(tempdir, file_info.name)
        with open(temp_file_path, "wb") as f:
            f.write(file_info.contents)
        os.utime(
            temp_file_path, (file_info.mtime.timestamp(), file_info.mtime.timestamp())
        )

        # xz creates <temp_file_path>.xz and preserves mtime.
        subprocess.run(["xz", temp_file_path], check=True, cwd=tempdir)

        compressed_file_on_temp = temp_file_path + ".xz"
        os.rename(compressed_file_on_temp, abs_archive_path)

        # Explicitly set the mtime of the archive file itself
        os.utime(
            abs_archive_path, (file_info.mtime.timestamp(), file_info.mtime.timestamp())
        )


def create_archive(archive_info: ArchiveInfo, base_dir: str):
    full_path = archive_info.get_archive_path(base_dir)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    if archive_info.generation_method == GenerationMethod.EXTERNAL:
        # Check that the archive file exists
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"External archive {full_path} does not exist")
        return

    # Assert that header_password is None for formats that don't support it
    if archive_info.generation_method in [
        GenerationMethod.ZIPFILE,
        GenerationMethod.INFOZIP,
        GenerationMethod.TAR_COMMAND_LINE,
        GenerationMethod.COMMAND_LINE,  # For gz, bz2, xz
    ]:
        assert archive_info.header_password is None, (
            f"Header password not supported for {archive_info.generation_method} / {archive_info.format}"
        )

    if archive_info.generation_method == GenerationMethod.COMMAND_LINE:
        if archive_info.format == ArchiveFormat.GZIP:
            create_gz_archive_with_command_line(
                full_path, archive_info.files, archive_info.archive_comment
            )
        elif archive_info.format == ArchiveFormat.BZIP2:
            create_bz2_archive_with_command_line(
                full_path, archive_info.files, archive_info.archive_comment
            )
        elif archive_info.format == ArchiveFormat.XZ:
            create_xz_archive_with_command_line(
                full_path, archive_info.files, archive_info.archive_comment
            )
        else:
            raise ValueError(
                f"Unsupported format {archive_info.format} for GenerationMethod.COMMAND_LINE"
            )
        return

    generator = GENERATION_METHODS_TO_GENERATOR[archive_info.generation_method]
    if archive_info.generation_method in [
        GenerationMethod.RAR_COMMAND_LINE,
        GenerationMethod.PY7ZR,
        GenerationMethod.SEVENZIP_COMMAND_LINE,
    ]:
        generator(
            full_path,
            archive_info.files,
            archive_info.archive_comment,
            archive_info.solid,
            archive_info.header_password,
        )
    elif archive_info.generation_method == GenerationMethod.TAR_COMMAND_LINE:
        generator(
            full_path,
            archive_info.files,
            archive_info.archive_comment,
            archive_info.format,
        )
    else:  # ZIPFILE, INFOZIP
        generator(full_path, archive_info.files, archive_info.archive_comment)


def create_rar_archive_with_command_line(
    archive_path: str,
    files: list[FileInfo],
    archive_comment: str | None = None,
    solid: bool = False,
    header_password: str | None = None,
):
    abs_archive_path = os.path.abspath(archive_path)
    if os.path.exists(abs_archive_path):
        os.remove(abs_archive_path)

    if solid and len(set(f.password for f in files)) > 1:
        raise ValueError("Solid archives do not support multiple passwords")

    if solid and len(set(f.compression_method for f in files)) > 1:
        raise ValueError("Solid archives do not support multiple compression methods")

    with tempfile.TemporaryDirectory() as tempdir:
        write_files_to_dir(tempdir, files)

        for i, (password, compression_method, group_files) in enumerate(
            group_files_by_password_and_compression_method(files)
        ):
            command = ["rar", "a", "-oh", "-ol"]

            if solid:
                command.append("-s")

            # Handle header password
            if header_password:
                command.append(f"-hp{header_password}")
                if password and password != header_password:
                    raise ValueError(
                        "Header password and file password cannot be different"
                    )

            # Handle file password
            elif password:
                command.append(f"-p{password}")

            # Handle archive comment
            comment_file_path = None
            if i == 0 and archive_comment:
                # rar expects the comment file to be passed with -z<file>
                comment_fd, comment_file_path = tempfile.mkstemp(dir=tempdir)
                with os.fdopen(comment_fd, "wb") as f:
                    f.write(archive_comment.encode("utf-8"))
                command.append(f"-z{comment_file_path}")

            command.append(abs_archive_path)

            # Add file names to the command (relative to tempdir)
            for file_info in group_files:
                # RAR typically includes directories implicitly if files within them are added.
                # However, to ensure empty directories or specific directory metadata (like mtime)
                # are preserved as defined in FileInfo, we add them explicitly.
                # RAR handles adding existing files/dirs.
                command.append(file_info.name)

            subprocess.run(command, check=True, cwd=tempdir)

            if comment_file_path:
                os.remove(comment_file_path)


def create_7z_archive_with_py7zr(
    archive_path: str,
    files: list[FileInfo],
    archive_comment: str | None = None,
    solid: bool = False,
    header_password: str | None = None,
):
    abs_archive_path = os.path.abspath(archive_path)
    if os.path.exists(abs_archive_path):
        os.remove(abs_archive_path)

    # In 7-zip, the solidness of the archive is determined by the files themselves.
    # Each archive has one or more "folders", which are groups of files that are
    # compressed together. An archive is considered solid if at least one folder has
    # more than one file.

    # When writing an archive, py7zr adds all files to the same folder. So, to create
    # a non-solid archive, we need to add each file individually and close the archive
    # after each one.

    if header_password and any(
        f.password is not None and f.password != header_password for f in files
    ):
        raise ValueError("Header password and file password cannot be different")

    with tempfile.TemporaryDirectory() as tempdir:
        write_files_to_dir(tempdir, files)

        file_groups: list[list[FileInfo]]
        if solid:
            file_groups = [files]
        else:
            # Create a separate group for each file, so it doesn't get compressed in the
            # same folder as another. But group dirs and symlinks along with a file
            # when writing, as they are not added to folders and the library breaks
            # if we don't add at least one actual file.
            file_groups = [[]]
            last_group_has_file = False
            for file in files:
                if file.type == MemberType.FILE and last_group_has_file:
                    file_groups.append([])
                    last_group_has_file = False
                file_groups[-1].append(file)
                if file.type == MemberType.FILE:
                    last_group_has_file = True

        for file_group in file_groups:
            for i, (password, compression_method, group_files) in enumerate(
                group_files_by_password_and_compression_method(file_group)
            ):
                # Use header password if provided, otherwise use file password

                with py7zr.SevenZipFile(
                    abs_archive_path,
                    "a",
                    password=header_password or password,
                    header_encryption=header_password is not None,
                ) as archive:
                    for file in group_files:
                        archive.write(os.path.join(tempdir, file.name), file.name)


def create_7z_archive_with_command_line(
    archive_path: str,
    files: list[FileInfo],
    archive_comment: str | None = None,
    solid: bool = False,
    header_password: str | None = None,
):
    if archive_comment:
        raise ValueError("Archive comments are not supported with 7z command line")

    abs_archive_path = os.path.abspath(archive_path)
    if os.path.exists(abs_archive_path):
        os.remove(abs_archive_path)

    with tempfile.TemporaryDirectory() as tempdir:
        write_files_to_dir(tempdir, files)

        for i, (password, compression_method, group_files) in enumerate(
            group_files_by_password_and_compression_method(files)
        ):
            command = ["7z", "a"]

            # Handle solid mode
            command.append(f"-ms={'on' if solid else 'off'}")

            if header_password:
                command.append(f"-p{header_password}")
                command.append("-mhe=on")  # Encrypt header
            elif password:
                command.append(f"-p{password}")

            command.append(abs_archive_path)

            # Add all contents of the temp directory. 7z handles path recursion.
            # Using "./*" or "." ensures that paths inside the archive are relative to the archive root.
            for file in group_files:
                command.append(file.name)

            subprocess.run(command, check=True, cwd=tempdir)


GENERATION_METHODS_TO_GENERATOR = {
    GenerationMethod.ZIPFILE: create_zip_archive_with_zipfile,
    GenerationMethod.INFOZIP: create_zip_archive_with_infozip_command_line,
    GenerationMethod.TAR_COMMAND_LINE: _create_tar_archive_with_tarfile, # Updated to use the new function
    GenerationMethod.RAR_COMMAND_LINE: create_rar_archive_with_command_line,
    GenerationMethod.PY7ZR: create_7z_archive_with_py7zr,
    GenerationMethod.SEVENZIP_COMMAND_LINE: create_7z_archive_with_command_line,
}


def filter_archives(
    archives: list[ArchiveInfo], patterns: list[str] | None
) -> list[ArchiveInfo]:
    """
    Filter archives based on filename patterns.
    If patterns is None or empty, return all archives.
    Takes the basename of each pattern to match against archive filenames.
    """
    if not patterns:
        return archives

    # Convert patterns to their basenames
    pattern_basenames = [os.path.basename(pattern) for pattern in patterns]

    filtered = []
    for archive in archives:
        if any(
            fnmatch.fnmatch(archive.filename, pattern) for pattern in pattern_basenames
        ):
            filtered.append(archive)
    return filtered


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate test archives")
    parser.add_argument(
        "patterns",
        nargs="*",
        help="Optional list of file patterns to generate. If not specified, generates all archives.",
    )
    parser.add_argument(
        "--base-dir",
        help="Base directory where archives will be generated. Defaults to the script directory.",
    )
    args = parser.parse_args()

    # Use base_dir if provided, otherwise use the directory of the script
    base_dir = (
        args.base_dir if args.base_dir else os.path.dirname(os.path.abspath(__file__))
    )

    # Filter archives based on patterns if provided
    archives_to_generate = filter_archives(SAMPLE_ARCHIVES, args.patterns)

    if not archives_to_generate:
        print("No matching archives found.")
        exit(1)

    logger.info(f"Generating {len(archives_to_generate)} archives:")
    for archive in archives_to_generate:
        create_archive(archive, base_dir)
        bullet = "-" if archive.generation_method != GenerationMethod.EXTERNAL else "s"
        logger.info(f"  {bullet} {archive.filename}")
