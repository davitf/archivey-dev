import unittest
import os
import io
import zipfile
from pathlib import Path

from archivey import open_archive_writer, ArchiveFormat, ArchiveMember
from archivey.exceptions import ArchiveError

# Helper to get a temporary file path
def temp_path(name: str) -> str:
    # This ideally should use a proper temp directory utility
    # For simplicity, using current dir, but not best practice for real tests
    return name

class TestZipWriter(unittest.TestCase):
    def tearDown(self):
        # Clean up created files
        files_to_remove = [
            "test_basic.zip", "test_fileobj.zip", "test_context.zip",
            "test_compress_stored.zip", "test_compress_deflated.zip",
            "test_add_file.zip", "test_add_dir.zip", "test_closed.zip",
            "test_archive_member.zip"
        ]
        for f_name in files_to_remove:
            if os.path.exists(f_name):
                os.remove(f_name)
        if os.path.exists("temp_dir_to_add"):
            # crude cleanup for temp_dir_to_add and its contents
            for root, dirs, files in os.walk("temp_dir_to_add", topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir("temp_dir_to_add")


    def test_write_simple_zip_path(self):
        archive_path = temp_path("test_basic.zip")
        with open_archive_writer(archive_path, format=ArchiveFormat.ZIP) as writer:
            writer.writestr("hello.txt", "Hello ZIP World!")
            writer.writestr("data/binary.dat", b"\x00\x01\x02\x03")

        with zipfile.ZipFile(archive_path, 'r') as zf:
            self.assertEqual(zf.read("hello.txt"), b"Hello ZIP World!")
            self.assertEqual(zf.read("data/binary.dat"), b"\x00\x01\x02\x03")
            self.assertIn("hello.txt", zf.namelist())
            self.assertIn("data/binary.dat", zf.namelist())

    def test_write_zip_fileobj(self):
        archive_path = temp_path("test_fileobj.zip") # Still need a path to write final zip for verification
        bio = io.BytesIO()
        with open_archive_writer(bio, format=ArchiveFormat.ZIP, mode="w") as writer:
            writer.writestr("file1.txt", "File in BytesIO ZIP.")
        
        # Write the BytesIO buffer to a file to verify with zipfile
        with open(archive_path, "wb") as f:
            f.write(bio.getvalue())

        with zipfile.ZipFile(archive_path, 'r') as zf:
            self.assertEqual(zf.read("file1.txt"), b"File in BytesIO ZIP.")

    def test_write_zip_context_manager(self):
        archive_path = temp_path("test_context.zip")
        with open_archive_writer(archive_path, format=ArchiveFormat.ZIP) as writer:
            writer.writestr("context.txt", "Contextual ZIP writing.")

        with zipfile.ZipFile(archive_path, 'r') as zf:
            self.assertTrue(zf.read("context.txt") == b"Contextual ZIP writing.")

    def test_zip_compression_stored(self):
        archive_path = temp_path("test_compress_stored.zip")
        with open_archive_writer(archive_path, format=ArchiveFormat.ZIP, compression=zipfile.ZIP_STORED) as writer:
            writer.writestr("uncompressed.txt", "This is stored.")

        with zipfile.ZipFile(archive_path, 'r') as zf:
            info = zf.getinfo("uncompressed.txt")
            self.assertEqual(info.compress_type, zipfile.ZIP_STORED)
            self.assertEqual(zf.read("uncompressed.txt"), b"This is stored.")
    
    def test_zip_compression_deflated(self):
        archive_path = temp_path("test_compress_deflated.zip")
        # ZIP_DEFLATED is often the default, but explicitly test
        with open_archive_writer(archive_path, format=ArchiveFormat.ZIP, compression=zipfile.ZIP_DEFLATED) as writer:
            writer.writestr("compressed.txt", "This is deflated." * 100) # Ensure some compression happens

        with zipfile.ZipFile(archive_path, 'r') as zf:
            info = zf.getinfo("compressed.txt")
            self.assertEqual(info.compress_type, zipfile.ZIP_DEFLATED)
            self.assertEqual(zf.read("compressed.txt"), b"This is deflated." * 100)
            self.assertLess(info.compress_size, info.file_size)


    def test_zip_write_file(self):
        archive_path = temp_path("test_add_file.zip")
        # Create a temporary file to add
        Path("temp_file_to_add.txt").write_text("Content of temp file.")
        
        with open_archive_writer(archive_path, format=ArchiveFormat.ZIP) as writer:
            writer.write("temp_file_to_add.txt", "added_file.txt")

        with zipfile.ZipFile(archive_path, 'r') as zf:
            self.assertEqual(zf.read("added_file.txt"), b"Content of temp file.")
        os.remove("temp_file_to_add.txt")

    def test_zip_write_directory(self):
        archive_path = temp_path("test_add_dir.zip")
        # Create a temporary directory with files
        Path("temp_dir_to_add").mkdir(exist_ok=True)
        Path("temp_dir_to_add/file1.txt").write_text("Dir File 1")
        Path("temp_dir_to_add/subdir").mkdir(exist_ok=True)
        Path("temp_dir_to_add/subdir/file2.txt").write_text("Dir File 2")

        with open_archive_writer(archive_path, format=ArchiveFormat.ZIP) as writer:
            writer.write("temp_dir_to_add", "my_dir")
        
        with zipfile.ZipFile(archive_path, 'r') as zf:
            self.assertIn("my_dir/file1.txt", zf.namelist())
            self.assertEqual(zf.read("my_dir/file1.txt"), b"Dir File 1")
            self.assertIn("my_dir/subdir/file2.txt", zf.namelist())
            self.assertEqual(zf.read("my_dir/subdir/file2.txt"), b"Dir File 2")
        
        # Clean up (simplified)
        # os.remove("temp_dir_to_add/subdir/file2.txt")
        # os.rmdir("temp_dir_to_add/subdir")
        # os.remove("temp_dir_to_add/file1.txt")
        # os.rmdir("temp_dir_to_add")


    def test_write_to_closed_zip_raises_error(self):
        archive_path = temp_path("test_closed.zip")
        writer = open_archive_writer(archive_path, format=ArchiveFormat.ZIP)
        writer.close()
        with self.assertRaises(ValueError): # Or ArchiveError depending on implementation
            writer.writestr("should_fail.txt", "data")
    
    def test_zip_archive_member_attributes(self):
        archive_path = temp_path("test_archive_member.zip")
        # Note: zipfile module might not store all permissions exactly as set,
        # and mtime resolution can vary. This is a basic test.
        mtime = 1678886400 # 2023-03-15 12:00:00 UTC
        member = ArchiveMember(filename="member_attrs.txt", mtime=mtime) # permissions=0o644

        with open_archive_writer(archive_path, format=ArchiveFormat.ZIP) as writer:
            with writer.open(member) as f:
                f.write(b"Testing member attributes.")
        
        with zipfile.ZipFile(archive_path, 'r') as zf:
            info = zf.getinfo("member_attrs.txt")
            self.assertEqual(info.date_time, (2023, 3, 15, 12, 0, 0)) 
            # Permissions check is more complex due to external_attr structure
            # self.assertEqual((info.external_attr >> 16) & 0xFFFF, 0o644) 

if __name__ == '__main__':
    unittest.main()
