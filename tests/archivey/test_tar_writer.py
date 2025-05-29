import unittest
import os
import io
import tarfile
from pathlib import Path

from archivey import open_archive_writer, ArchiveFormat, ArchiveMember, MemberType
from archivey.exceptions import ArchiveError

def temp_path(name: str) -> str:
    return name # Simplified

class TestTarWriter(unittest.TestCase):
    def tearDown(self):
        files_to_remove = [
            "test_basic.tar", "test_basic.tar.gz", "test_basic.tar.bz2", "test_basic.tar.xz",
            "test_fileobj.tar", "test_context.tar", "test_add_file.tar", "test_add_dir.tar",
            "test_closed.tar", "test_archive_member.tar", "test_links_and_dirs.tar"
        ]
        for f_name in files_to_remove:
            if os.path.exists(f_name):
                os.remove(f_name)
        if os.path.exists("temp_dir_to_add_tar"):
            # crude cleanup for temp_dir_to_add_tar and its contents
            for root, dirs, files in os.walk("temp_dir_to_add_tar", topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir("temp_dir_to_add_tar")

    def _verify_tar_contents(self, archive_path, expected_files, compression_mode="r"):
        with tarfile.open(archive_path, compression_mode) as tf:
            members = tf.getnames()
            for name, expected_content in expected_files.items():
                self.assertIn(name, members)
                if expected_content is not None: # None for dirs/links where content is not checked this way
                    member_file = tf.extractfile(name)
                    self.assertIsNotNone(member_file)
                    self.assertEqual(member_file.read(), expected_content)

    def test_write_simple_tar_path(self):
        archive_path = temp_path("test_basic.tar")
        expected = {"hello.txt": b"Hello TAR World!", "data/binary.dat": b"\x00\x01\x02\x03"}
        with open_archive_writer(archive_path, format=ArchiveFormat.TAR) as writer:
            writer.writestr("hello.txt", "Hello TAR World!")
            writer.writestr("data/binary.dat", b"\x00\x01\x02\x03")
        self._verify_tar_contents(archive_path, expected, "r:")
    
    def test_write_tar_gz(self):
        archive_path = temp_path("test_basic.tar.gz")
        expected = {"gzipped.txt": b"This is gzipped TAR."}
        # mode="w:gz" or format=ArchiveFormat.TAR_GZ
        with open_archive_writer(archive_path, format=ArchiveFormat.TAR_GZ) as writer:
            writer.writestr("gzipped.txt", "This is gzipped TAR.")
        self._verify_tar_contents(archive_path, expected, "r:gz")

    def test_write_tar_bz2(self):
        archive_path = temp_path("test_basic.tar.bz2")
        expected = {"bzipped.txt": b"This is bzipped2 TAR."}
        with open_archive_writer(archive_path, format=ArchiveFormat.TAR_BZ2) as writer:
            writer.writestr("bzipped.txt", "This is bzipped2 TAR.")
        self._verify_tar_contents(archive_path, expected, "r:bz2")

    @unittest.skipIf(not tarfile.HAS_LZMA, "lzma module not available")
    def test_write_tar_xz(self):
        archive_path = temp_path("test_basic.tar.xz")
        expected = {"xzipped.txt": b"This is xz/lzma TAR."}
        with open_archive_writer(archive_path, format=ArchiveFormat.TAR_XZ) as writer:
            writer.writestr("xzipped.txt", "This is xz/lzma TAR.")
        self._verify_tar_contents(archive_path, expected, "r:xz")

    def test_write_tar_fileobj(self):
        archive_path = temp_path("test_fileobj.tar")
        bio = io.BytesIO()
        # For fileobj with TAR, format is crucial if mode doesn't specify compression
        with open_archive_writer(bio, format=ArchiveFormat.TAR, mode="w") as writer:
            writer.writestr("file1.txt", "File in BytesIO TAR.")
        
        with open(archive_path, "wb") as f:
            f.write(bio.getvalue())
        self._verify_tar_contents(archive_path, {"file1.txt": b"File in BytesIO TAR."}, "r:")

    def test_write_tar_context_manager(self):
        archive_path = temp_path("test_context.tar")
        with open_archive_writer(archive_path, format=ArchiveFormat.TAR) as writer:
            writer.writestr("context.txt", "Contextual TAR writing.")
        self._verify_tar_contents(archive_path, {"context.txt": b"Contextual TAR writing."}, "r:")

    def test_tar_write_file(self):
        archive_path = temp_path("test_add_file.tar")
        Path("temp_file_to_add_tar.txt").write_text("Content of TAR temp file.")
        
        with open_archive_writer(archive_path, format=ArchiveFormat.TAR) as writer:
            writer.write("temp_file_to_add_tar.txt", "added_tar_file.txt")
        
        self._verify_tar_contents(archive_path, {"added_tar_file.txt": b"Content of TAR temp file."}, "r:")
        os.remove("temp_file_to_add_tar.txt")

    def test_tar_write_directory(self):
        archive_path = temp_path("test_add_dir.tar")
        Path("temp_dir_to_add_tar").mkdir(exist_ok=True)
        Path("temp_dir_to_add_tar/file1.txt").write_text("Dir TAR File 1")
        Path("temp_dir_to_add_tar/subdir_tar").mkdir(exist_ok=True)
        Path("temp_dir_to_add_tar/subdir_tar/file2.txt").write_text("Dir TAR File 2")

        with open_archive_writer(archive_path, format=ArchiveFormat.TAR) as writer:
            writer.write("temp_dir_to_add_tar", "my_tar_dir")
        
        expected = {
            "my_tar_dir/file1.txt": b"Dir TAR File 1",
            "my_tar_dir/subdir_tar/file2.txt": b"Dir TAR File 2"
        }
        self._verify_tar_contents(archive_path, expected, "r:")


    def test_write_to_closed_tar_raises_error(self):
        archive_path = temp_path("test_closed.tar")
        writer = open_archive_writer(archive_path, format=ArchiveFormat.TAR)
        writer.close()
        with self.assertRaises(ValueError): # Or ArchiveError
            writer.writestr("should_fail.txt", "data")

    def test_tar_archive_member_attributes(self):
        archive_path = temp_path("test_archive_member.tar")
        mtime = 1678886400.0 # 2023-03-15 12:00:00 UTC
        permissions = 0o755
        member = ArchiveMember(filename="member_attrs.txt", mtime=mtime, permissions=permissions)

        with open_archive_writer(archive_path, format=ArchiveFormat.TAR) as writer:
            with writer.open(member) as f:
                f.write(b"Testing member attributes for TAR.")
        
        with tarfile.open(archive_path, "r:") as tf:
            info = tf.getmember("member_attrs.txt")
            self.assertEqual(info.mtime, int(mtime))
            self.assertEqual(info.mode, permissions)
            # Verify content as well
            self.assertEqual(tf.extractfile(info).read(), b"Testing member attributes for TAR.")
    
    def test_tar_write_links_and_dirs(self):
        archive_path = temp_path("test_links_and_dirs.tar")
        dir_member = ArchiveMember("my_empty_dir/", type=MemberType.DIR, mtime=1234567890)
        # Note: TarWriter's open for non-REGTYPE might not use the stream if size is 0
        # For symlinks, link_target is key.
        link_member = ArchiveMember("my_link", type=MemberType.LINK, link_target="target_file.txt", mtime=1234567891)

        with open_archive_writer(archive_path, format=ArchiveFormat.TAR) as writer:
            # Adding a directory (TarInfo type will be DIRTYPE)
            # For TarWriter, if open() is used for a DIR, the stream isn't really used.
            # It's more about creating the TarInfo correctly.
            # A more direct add_member(tarinfo) might be better for non-file types.
            # Current `write` method in base_writer doesn't create empty dir entries explicitly.
            # Let's test adding via open() using ArchiveMember.
            with writer.open(dir_member) as f:
                pass # No data to write for a directory entry itself

            with writer.open(link_member) as f:
                pass # No data to write for a symlink entry itself

            writer.writestr("target_file.txt", "This is the target.")


        with tarfile.open(archive_path, "r:") as tf:
            dir_info = tf.getmember("my_empty_dir/")
            self.assertTrue(dir_info.isdir())
            self.assertEqual(dir_info.mtime, 1234567890)

            link_info = tf.getmember("my_link")
            self.assertTrue(link_info.issym())
            self.assertEqual(link_info.linkname, "target_file.txt")
            self.assertEqual(link_info.mtime, 1234567891)
            
            target_info = tf.getmember("target_file.txt")
            self.assertTrue(target_info.isfile())


if __name__ == '__main__':
    unittest.main()
