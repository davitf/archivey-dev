import logging
import os
import pathlib
import shutil
import pytest

from archivey.core import open_archive, open_compressed_stream
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
    ArchiveMemberCannotBeOpenedError,
    PackageNotInstalledError,
)
from archivey.types import (
    SINGLE_FILE_COMPRESSED_FORMATS,
    ArchiveFormat,
    MemberType,
)
from tests.archivey.test_samples import (
    ALTERNATIVE_CONFIG,
    ALTERNATIVE_PACKAGES_FORMATS,
    BASIC_ARCHIVES,
    SAMPLE_ARCHIVES,
    SINGLE_FILE_ARCHIVES,
    ArchiveContents,
    File,
    SampleArchive,
    filter_archives,
)
from tests.archivey.create_archives import (
    create_7z_archive_with_py7zr,
    create_rar_archive_with_command_line,
    create_tar_archive_with_tarfile,
    create_zip_archive_with_zipfile,
)
from tests.archivey.test_io import EXPECTED_NON_SEEKABLE_FAILURES
from tests.archivey.test_utils import skip_if_package_missing
from tests.create_corrupted_archives import corrupt_archive


def _prepare_corrupted_archive(
    sample_archive: SampleArchive,
    sample_archive_path: str,
    tmp_path_factory: pytest.TempPathFactory,
    corruption_type: str,
) -> pathlib.Path:
    """Return path to a corrupted version of the sample archive."""
    path = pathlib.Path(
        sample_archive.get_archive_path(variant=f"corrupted_{corruption_type}")
    )
    if path.exists():
        return path

    output_dir = tmp_path_factory.mktemp("generated_archives")
    corrupted_archive_path = output_dir / sample_archive.get_archive_name(
        variant=f"corrupted_{corruption_type}"
    )
    logger.info(
        f"Creating corrupted archive {corrupted_archive_path} with corruption type {corruption_type}"
    )
    corrupt_archive(
        pathlib.Path(sample_archive_path),
        corrupted_archive_path,
        corruption_type=corruption_type,
    )
    return corrupted_archive_path


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["large_files_nonsolid", "large_files_solid", "large_single_file"],
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize("corruption_type", ["random", "zeroes", "ffs"])
@pytest.mark.parametrize("read_streams", [True, False], ids=["read", "noread"])
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_read_corrupted_archives(
    sample_archive: SampleArchive,
    sample_archive_path: str,
    tmp_path_factory: pytest.TempPathFactory,
    read_streams: bool,
    alternative_packages: bool,
    corruption_type: str,
):
    """Test that reading generally corrupted archives raises ArchiveCorruptedError.

    Args:
        sample_archive: The archive to test
        sample_archive_path: Path to the source archive
        corruption_type: Type of corruption applied:
            - "random": Byte range replaced with random data
            - "zeroes": Byte range replaced with zeros
            - "ffs": Byte range replaced with 0xFF
    """
    if alternative_packages:
        if sample_archive.creation_info.format not in ALTERNATIVE_PACKAGES_FORMATS:
            pytest.skip("No alternative package for this format, no need to test")
        config = ALTERNATIVE_CONFIG
    else:
        config = None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    formats_without_redundancy_check = [
        ArchiveFormat.LZ4,
        ArchiveFormat.TAR,
    ]

    if sample_archive.creation_info.format == ArchiveFormat.FOLDER:
        pytest.skip("Folder archives cannot be corrupted")

    corrupted_archive_path = _prepare_corrupted_archive(
        sample_archive,
        sample_archive_path,
        tmp_path_factory,
        corruption_type,
    )

    try:
        found_member_names = []
        found_member_data = {}

        with open_archive(
            corrupted_archive_path, config=config, streaming_only=True
        ) as archive:
            for member, stream in archive.iter_members_with_streams():
                logger.info(f"Reading member {member.filename}")
                filename = member.filename

                # Single file formats don't store the filename, and the reader derives
                # it from the archive name. But here, the archive name has a
                # .corrupted_xxx suffix that doesn't match the name in sample_archive,
                # so we need to remove it.
                if (
                    sample_archive.creation_info.format
                    in SINGLE_FILE_COMPRESSED_FORMATS
                ):
                    filename = os.path.splitext(filename)[0]

                if stream is not None and read_streams:
                    data = stream.read()
                    logger.info(f"Read {len(data)} bytes from member {filename}")

                    found_member_data[filename] = data

                found_member_names.append(filename)

        expected_member_data = {
            member.name: member.contents for member in sample_archive.contents.files
        }
        logger.info(f"{found_member_names=}, expected={expected_member_data.keys()}")

        if (
            not read_streams
            and archive.format == ArchiveFormat.BZIP2
            and sample_archive.creation_info.format == ArchiveFormat.TAR_BZ2
        ):
            # In some corrupted archives, bz2 can uncompress the data stream, but it's
            # not a valid tar format. If we don't actually attempt to read the streams,
            # we won't detect the corruption.
            pytest.xfail(
                "Bzip2 can uncompress the data stream, but it's not a valid tar format."
            )

        # If no error was raised, it likely means that the corruption didn't affect the
        # archive directory or member metadata, so at least all the members should have
        # been read.
        assert set(found_member_names) == set(expected_member_data.keys()), (
            f"Archive {corrupted_archive_path} did not raise an error but did not read all members"
        )

        if read_streams:
            assert (
                sample_archive.creation_info.format in formats_without_redundancy_check
            ), f"Archive {corrupted_archive_path} should have detected a corruption"
            # If we read the streams and an error wasn't raised, it means the compressed
            # stream was valid, but at least one member should have different data.
            broken_files = [
                name
                for name, contents in expected_member_data.items()
                if contents is not None and contents != found_member_data[name]
            ]
            assert len(broken_files) >= 1, (
                f"Archive {corrupted_archive_path} should have at least one broken file"
            )
            # If this is a multi-file archive, which we corrupted in the middle,
            # at least the first file should be good. The last may or may not be broken,
            # depending on how the error was propagated.
            if len(expected_member_data) >= 1:
                assert len(broken_files) <= len(expected_member_data), (
                    f"Archive {corrupted_archive_path} should have at least one good file"
                )

    except (ArchiveCorruptedError, ArchiveEOFError):
        logger.info(f"Archive {corrupted_archive_path} raised an error", exc_info=True)


@pytest.mark.parametrize("corrupted_length", [16, 47, 0.1, 0.9])
@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["large_files_nonsolid", "large_files_solid", "large_single_file"],
        # Tar files don't have any kind of error detection, so we skip them.
        # custom_filter=lambda a: a.creation_info.format != ArchiveFormat.TAR,
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize("read_streams", [True, False], ids=["read", "noread"])
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_read_truncated_archives(
    sample_archive: SampleArchive,
    corrupted_length: int | float,
    tmp_path_factory: pytest.TempPathFactory,
    read_streams: bool,
    alternative_packages: bool,
):
    """Test that reading truncated archives raises appropriate errors."""
    if sample_archive.creation_info.format == ArchiveFormat.FOLDER:
        pytest.skip("Folder archives cannot be truncated")

    if alternative_packages:
        if sample_archive.creation_info.format not in ALTERNATIVE_PACKAGES_FORMATS:
            pytest.skip("No alternative package for this format, no need to test")
        config = ALTERNATIVE_CONFIG
    else:
        config = None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    filename = sample_archive.get_archive_name(variant=f"truncated_{corrupted_length}")
    output_path = tmp_path_factory.mktemp("generated_archives") / filename

    logger.info(
        f"Testing truncated archive {output_path} with length {corrupted_length}"
    )

    data = open(sample_archive.get_archive_path(), "rb").read()
    if isinstance(corrupted_length, float):
        corrupted_length = int(corrupted_length * len(data))

    with open(output_path, "wb") as f:
        f.write(data[:corrupted_length])

    try:
        with open_archive(output_path, config=config, streaming_only=True) as archive:
            for member, stream in archive.iter_members_with_streams():
                if stream is not None and read_streams:
                    stream.read()
        logger.warning(f"Archive {output_path} did not raise an error")
    except (ArchiveCorruptedError, ArchiveEOFError):
        # Test passes if one of the expected exceptions is raised
        pass


def compress_file(src: str, dst: str, fmt: ArchiveFormat) -> str:
    opener = SINGLE_FILE_LIBRARY_OPENERS.get(fmt)
    if opener is None:
        pytest.skip(f"Required library for {fmt.name} is not installed")
    with open(src, "rb") as f_in, opener(dst, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    return dst


def create_archive_with_member(
    outer_format: ArchiveFormat, inner_path: str, dst: str
) -> str:
    data = open(inner_path, "rb").read()
    contents = ArchiveContents(
        file_basename="outer", files=[File(os.path.basename(inner_path), 1, data)]
    )

    if outer_format == ArchiveFormat.ZIP:
        create_zip_archive_with_zipfile(dst, contents, ArchiveFormat.ZIP)
    elif outer_format == ArchiveFormat.RAR:
        create_rar_archive_with_command_line(dst, contents, ArchiveFormat.RAR)
    elif outer_format == ArchiveFormat.SEVENZIP:
        create_7z_archive_with_py7zr(dst, contents, ArchiveFormat.SEVENZIP)
    elif outer_format in [
        ArchiveFormat.TAR_GZ,
        ArchiveFormat.TAR_BZ2,
        ArchiveFormat.TAR_XZ,
        ArchiveFormat.TAR_ZSTD,
        ArchiveFormat.TAR_LZ4,
        ArchiveFormat.TAR,
    ]:
        create_tar_archive_with_tarfile(dst, contents, outer_format)
    else:
        raise AssertionError(f"Unsupported outer format {outer_format}")
    return dst


logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "outer_format",
    SINGLE_FILE_LIBRARY_OPENERS.keys(),
)
@pytest.mark.parametrize(
    "inner_archive",
    filter_archives(
        BASIC_ARCHIVES + SINGLE_FILE_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format != ArchiveFormat.FOLDER,
    ),
    # TAR_MEMBER_PAIRS,
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["default", "altlibs"]
)
def test_open_archive_from_compressed_stream(
    outer_format: ArchiveFormat,
    inner_archive: SampleArchive,
    tmp_path,
    alternative_packages: bool,
):
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(outer_format, config)
    skip_if_package_missing(inner_archive.creation_info.format, config)

    if (
        alternative_packages
        and outer_format == ArchiveFormat.BZIP2
        and inner_archive.filename.endswith(".bz2")
    ):
        pytest.xfail("prevent segfault")

    logger.info(
        f"alternative_packages: {alternative_packages}, outer_format: {outer_format}, inner_archive.filename: {inner_archive.filename}"
    )

    inner_path = inner_archive.get_archive_path()
    compressed_path = os.path.join(
        tmp_path, os.path.basename(inner_path) + "." + outer_format.value
    )
    compress_file(inner_path, compressed_path, outer_format)

    with open_compressed_stream(compressed_path, config=config) as stream:
        with open_archive(stream, config=config, streaming_only=True) as archive:
            assert archive.format == inner_archive.creation_info.format
            has_member = False
            for _, member_stream in archive.iter_members_with_streams():
                has_member = True
                if member_stream is not None:
                    member_stream.read()
            assert has_member


@pytest.mark.parametrize(
    "outer_format",
    [
        ArchiveFormat.TAR_GZ,
        ArchiveFormat.TAR_BZ2,
        ArchiveFormat.TAR_XZ,
        ArchiveFormat.TAR_ZSTD,
        ArchiveFormat.TAR_LZ4,
        ArchiveFormat.TAR,
        ArchiveFormat.ZIP,
        ArchiveFormat.RAR,
        ArchiveFormat.SEVENZIP,
    ],
    # ids=TAR_MEMBER_IDS,
)
@pytest.mark.parametrize(
    "inner_archive",
    filter_archives(
        BASIC_ARCHIVES + SINGLE_FILE_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format != ArchiveFormat.FOLDER,
    ),
    # TAR_MEMBER_PAIRS,
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["default", "altlibs"]
)
def test_open_archive_from_member(
    outer_format: ArchiveFormat,
    inner_archive: SampleArchive,
    tmp_path,
    alternative_packages: bool,
):
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(outer_format, config)
    skip_if_package_missing(inner_archive.creation_info.format, config)

    inner_path = inner_archive.get_archive_path()
    outer_path = os.path.join(tmp_path, "outer." + outer_format.value)
    try:
        create_archive_with_member(outer_format, inner_path, outer_path)
    except PackageNotInstalledError as exc:
        pytest.skip(str(exc))

    with open_archive(outer_path, config=config, streaming_only=True) as outer:
        for member, stream in outer.iter_members_with_streams():
            assert member.filename.endswith(os.path.basename(inner_path))
            assert stream is not None

            if (
                not stream.seekable()
                and (inner_archive.creation_info.format, alternative_packages)
                in EXPECTED_NON_SEEKABLE_FAILURES
            ):
                pytest.xfail("Non-seekable stream not supported")

            with open_archive(stream, config=config, streaming_only=True) as archive:
                assert archive.format == inner_archive.creation_info.format
                for _ in archive.iter_members_with_streams():
                    break
            break


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, prefixes=["symlink_loop"]),
    ids=lambda x: x.filename,
)
def test_symlink_loop_archives(sample_archive: SampleArchive, sample_archive_path: str):
    """Ensure that archives with symlink loops do not cause infinite loops."""
    with open_archive(sample_archive_path) as archive:
        for member in archive.get_members():
            if member.type == MemberType.SYMLINK:
                if member.link_target == "file5.txt":
                    with archive.open(member) as fh:
                        fh.read()
                else:
                    with pytest.raises(ArchiveMemberCannotBeOpenedError):
                        archive.open(member)
            else:
                with archive.open(member) as fh:
                    fh.read()
