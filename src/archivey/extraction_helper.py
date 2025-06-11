import collections
import logging
import os
import shutil
from typing import BinaryIO

from archivey.config import OverwriteMode
from archivey.exceptions import ArchiveFileExistsError, ArchiveLinkTargetNotFoundError
from archivey.types import ArchiveMember, MemberType

logger = logging.getLogger(__name__)


def apply_member_metadata(member: ArchiveMember, target_path: str) -> None:
    if member.mtime:
        os.utime(target_path, (member.mtime.timestamp(), member.mtime.timestamp()))

    if member.mode:
        os.chmod(target_path, member.mode)


class ExtractionHelper:
    def __init__(
        self,
        archive_name: str,
        root_path: str,
        overwrite_mode: OverwriteMode,
        can_process_pending_extractions: bool = True,
    ):
        self.archive_name = archive_name
        self.root_path = root_path
        self.overwrite_mode = overwrite_mode
        self.can_process_pending_extractions = can_process_pending_extractions

        assert isinstance(self.overwrite_mode, OverwriteMode)

        self.extracted_members_by_path: dict[str, ArchiveMember] = {}
        self.extracted_path_by_source_id: dict[int, str] = {}

        # List of (member, path) tuples that were not extracted because they were
        # pointing to a file that doesn't exist in the filesystem.
        # self.pending_hardlinks_to_create: list[tuple[ArchiveMember, str]] = []

        self.failed_extractions: list[ArchiveMember] = []

        self.pending_files_to_extract_by_id: dict[int, ArchiveMember] = {}
        self.pending_target_members_by_source_id: dict[int, list[ArchiveMember]] = (
            collections.defaultdict(list)
        )

    def get_output_path(self, member: ArchiveMember) -> str:
        return os.path.normpath(os.path.join(self.root_path, member.filename))

    def check_overwrites(self, member: ArchiveMember, path: str) -> bool:
        # TODO: should we handle the case where some entry in the path to the file
        # is actually a symlink pointing outside the root path? Is that a possible
        # security issue?

        if not os.path.lexists(path):
            # File doesn't exist, nothing to do
            return True

        existing_file_is_dir = os.path.isdir(path)
        if member.type == MemberType.DIR and existing_file_is_dir:
            # No problem, we're overwriting a directory with a directory
            return True

        if path in self.extracted_members_by_path:
            # The file was created during this extraction, so we can overwrite it regardless
            # of the overwrite mode.
            logger.info(
                f"Overwriting existing {member.type.value} {path} as it was created during this extraction"
            )

        elif self.overwrite_mode == OverwriteMode.SKIP:
            logger.info(f"Skipping existing {member.type.value} {path}")
            self.failed_extractions.append(member)
            return False

        elif self.overwrite_mode == OverwriteMode.ERROR:
            self.failed_extractions.append(member)
            raise ArchiveFileExistsError(f"{member.type.value} {path} already exists")

        if member.type == MemberType.DIR:
            # This is only reached if the member is a directory and the existing file is not
            self.failed_extractions.append(member)
            raise ArchiveFileExistsError(
                f"Cannot create dir {path} as it already exists as a file"
            )

        if existing_file_is_dir:
            self.failed_extractions.append(member)
            raise ArchiveFileExistsError(
                f"Cannot create {member.type.value} {path} as it already exists as a dir"
            )

        os.remove(path)

        return True

    def create_directory(self, member: ArchiveMember, path: str) -> bool:
        if not self.check_overwrites(member, path):
            return False

        os.makedirs(path, exist_ok=True)
        self.extracted_members_by_path[path] = member
        return True

    def process_file_extracted(self, member: ArchiveMember, normpath: str) -> None:
        """Called for files that had a delayed extraction."""
        if member.is_link:
            return self.process_link_extracted(member, normpath)

        assert member.is_file

        targets = self.pending_target_members_by_source_id.get(member.internal_id)
        if not targets:
            # We were not expecting this file to be extracted. TODO: should we delete it?
            logger.error(
                f"Unexpected file {member.filename} was extracted by an external library"
            )
            return

        self.pending_files_to_extract_by_id.pop(member.internal_id, None)

        for i, target in enumerate(targets):
            # TODO: handle exceptions

            target_path = self.get_output_path(target)

            if i == 0:
                # The first target is either the original member or, if it was not
                # extracted, the first hardlink that pointed to it, but which should become a regular file.
                # In both cases, move the file if it is not in the expected location
                # (which can happen even for the original member, if the library renamed it
                # if there were several files with the same name -- py7zr does this,
                # or if the filter function renamed it).
                if os.path.realpath(target_path) != os.path.realpath(normpath):
                    self.check_overwrites(member, target_path)
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    shutil.move(normpath, target_path)

            else:
                # Create a hardlink to the first target.
                try:
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    os.link(target_path, self.get_output_path(target))

                except (AttributeError, NotImplementedError, OSError):
                    # os.link failed, so we need to create a copy as a regular file.
                    # The list of exceptions was taken from tarfile.py.
                    logger.info(
                        f"Creating hardlink for {target.filename} failed, copying the file instead"
                    )
                    shutil.copyfile(normpath, target_path)

            # Remove the file from the pending list.
            self.pending_target_members_by_source_id[member.internal_id].remove(target)
            self.extracted_path_by_source_id[target.internal_id] = target_path
            self.extracted_members_by_path[target_path] = target

    def create_regular_file(
        self, member: ArchiveMember, stream: BinaryIO | None, path: str
    ) -> bool:
        if not self.check_overwrites(member, path):
            return False

        if stream is None:
            # This is a delayed extraction, so we need to store the member and the path
            # for later.
            self.pending_files_to_extract_by_id[member.internal_id] = member
            self.pending_target_members_by_source_id[member.internal_id].append(member)
            return True

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as dst:
            shutil.copyfileobj(stream, dst)
        self.extracted_members_by_path[path] = member
        self.extracted_path_by_source_id[member.internal_id] = path

        if member.internal_id in self.pending_target_members_by_source_id:
            self.process_file_extracted(member, path)

        return True

    def create_link(self, member: ArchiveMember, member_path: str) -> bool:
        logger.error(
            f"Creating link {member.filename} to {member.link_target} , path={member_path}"
        )
        if member.link_target is None:
            # The link target may not have been read yet (possible for 7z archives)
            if self.can_process_pending_extractions:
                logger.info(
                    f"Link target not set for {member.filename}, storing for later extraction"
                )
                self.pending_files_to_extract_by_id[member.internal_id] = member

                return True
            else:
                logger.error(f"Link target not set for {member.filename}")
                self.failed_extractions.append(member)
                return False

        if member.type == MemberType.HARDLINK:
            # Hard links can only point to files in the same archive.
            # If that file was already extracted, take the target path from the extracted path
            target_member = member.link_target_member
            if target_member is None:
                self.failed_extractions.append(member)
                raise ArchiveLinkTargetNotFoundError(
                    f"Hardlink target {member.link_target} not found for {member.filename}"
                )

            target_path = self.extracted_path_by_source_id.get(
                target_member.internal_id
            )
            if target_path is None:
                # The target file was not extracted, so we need to store it for later
                # extraction if possible.
                if self.can_process_pending_extractions:
                    logger.info(
                        f"Storing hardlink {member.filename} for later extraction as its target {target_member.filename} was not extracted"
                    )
                    self.pending_files_to_extract_by_id[target_member.internal_id] = (
                        target_member
                    )
                    self.pending_target_members_by_source_id[
                        target_member.internal_id
                    ].append(member)
                    return True
                else:
                    logger.error(
                        f"Hardlink target {member.link_target} was not extracted for {member.filename}"
                    )
                    self.failed_extractions.append(member)
                    return False

        elif member.type == MemberType.SYMLINK:
            symlink_dir = os.path.dirname(os.path.join(self.root_path, member.filename))
            target_path = os.path.normpath(
                os.path.join(symlink_dir, member.link_target)
            )

        else:
            raise ValueError(f"Unexpected member type: {member.type}")

        if os.path.realpath(member_path) == os.path.realpath(target_path):
            # .tar files can contain links to themselves, which is not a problem,
            # but we can't remove the previous file in this case as there would be
            # nowhere to point to.
            logger.info(f"Skipping {member.type.value} to self: {member.filename}")
            return True

        if not self.check_overwrites(member, member_path):
            return False

        os.makedirs(os.path.dirname(member_path), exist_ok=True)
        if member.type == MemberType.HARDLINK:
            os.link(target_path, member_path)
        else:
            os.symlink(
                member.link_target,
                member_path,
                target_is_directory=member.link_target_type == MemberType.DIR,
            )
        self.extracted_members_by_path[member_path] = member
        return True

    def process_link_extracted(self, member: ArchiveMember) -> None:
        if member.link_target is None:
            raise ValueError(
                f"Link target not set for {member.filename} after external extraction"
            )

        self.create_link(member, self.get_output_path(member))

    def extract_member(self, member: ArchiveMember, stream: BinaryIO | None) -> bool:
        path = self.get_output_path(member)
        logger.info(
            f"Extracting {member.filename} to {path}, stream: {stream is not None}"
        )

        if member.is_dir:
            return self.create_directory(member, path)

        elif member.is_file:
            return self.create_regular_file(member, stream, path)

        elif member.is_link:
            return self.create_link(member, path)

        else:
            self.failed_extractions.append(member)
            logger.error(f"Unexpected member type: {member.type}")
            return False

    def process_external_extraction(self, member: ArchiveMember, rel_path: str) -> None:
        """Called for files that were extracted by an external library."""
        full_path = os.path.realpath(os.path.join(self.root_path, rel_path))
        self.process_file_extracted(member, full_path)

    def get_pending_extractions(self) -> list[ArchiveMember]:
        logger.info(
            f"Getting pending extractions: {', '.join(f'{k}: {v.filename} ({v.type.value})' for k, v in self.pending_files_to_extract_by_id.items())}"
        )
        return list(self.pending_files_to_extract_by_id.values())

    def get_failed_extractions(self) -> list[ArchiveMember]:
        return self.failed_extractions

    def apply_metadata(self) -> None:
        for path, member in self.extracted_members_by_path.items():
            apply_member_metadata(member, path)
