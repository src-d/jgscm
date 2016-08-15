from datetime import datetime
from unittest import main, TestCase
import uuid

from jgscm import GoogleStorageContentManager


class TestGoogleStorageContentManager(TestCase):
    BUCKET = "%s-%s" % ("jgcsm", uuid.uuid4())

    @classmethod
    def setUpClass(cls):
        GoogleStorageContentManager().client.bucket(cls.BUCKET).create()

    @classmethod
    def tearDownClass(cls):
        GoogleStorageContentManager().client.bucket(cls.BUCKET).delete(
            force=True)

    def setUp(self):
        super(TestGoogleStorageContentManager, self).setUp()
        self.contents_manager = GoogleStorageContentManager()

    @property
    def bucket(self):
        return self.contents_manager._get_bucket(self.BUCKET)

    def path(self, sub):
        return "/" + self.BUCKET + "/" + sub

    def test_file_exists(self):
        self.assertFalse(self.contents_manager.file_exists(""))
        self.assertFalse(self.contents_manager.file_exists("/"))
        self.assertFalse(self.contents_manager.file_exists(self.BUCKET))
        self.assertFalse(self.contents_manager.file_exists(self.BUCKET + "/"))
        bucket = self.bucket
        blob = bucket.blob("test")
        blob.upload_from_string(b"test")
        try:
            self.assertTrue(self.contents_manager.file_exists(self.path(
                "test")))
            self.assertFalse(self.contents_manager.file_exists(self.path(
                "test_")))
        finally:
            blob.delete()
        blob = bucket.blob("test/other")
        blob.upload_from_string(b"test")
        try:
            self.assertFalse(self.contents_manager.file_exists(self.path(
                "test")))
            self.assertFalse(self.contents_manager.file_exists(self.path(
                "test/")))
            self.assertTrue(self.contents_manager.file_exists(self.path(
                "test/other")))
            self.assertFalse(self.contents_manager.file_exists(self.path(
                "test/other/")))
        finally:
            blob.delete()

    def test_dir_exists(self):
        self.assertTrue(self.contents_manager.dir_exists(""))
        self.assertTrue(self.contents_manager.dir_exists("/"))
        self.assertTrue(self.contents_manager.dir_exists("/" + self.BUCKET))
        self.assertTrue(self.contents_manager.dir_exists(self.BUCKET))
        self.assertTrue(self.contents_manager.dir_exists(
            "/" + self.BUCKET + "/"))
        self.assertTrue(self.contents_manager.dir_exists(self.BUCKET + "/"))
        self.assertFalse(self.contents_manager.dir_exists(
            self.BUCKET + "/" + "test"))
        self.assertFalse(self.contents_manager.dir_exists(
            "/" + self.BUCKET + "/" + "test"))
        self.assertFalse(self.contents_manager.dir_exists(
            self.BUCKET + "blahblah"))
        self.assertFalse(self.contents_manager.dir_exists(
            "/" + self.BUCKET + "blahblah"))
        bucket = self.bucket
        blob = bucket.blob("test")
        blob.upload_from_string(b"test")
        try:
            self.assertFalse(self.contents_manager.dir_exists(self.path(
                "wtf")))
            self.assertFalse(self.contents_manager.dir_exists(self.path(
                "test")))
            self.assertFalse(self.contents_manager.dir_exists(self.path(
                "test/")))
        finally:
            blob.delete()
        blob = bucket.blob("test/")
        blob.upload_from_string(b"")
        try:
            self.assertTrue(self.contents_manager.dir_exists(self.path(
                "test")))
            self.assertTrue(self.contents_manager.dir_exists(self.path(
                "test/")))
        finally:
            blob.delete()
        blob = bucket.blob("test/other/")
        blob.upload_from_string(b"")
        try:
            self.assertTrue(self.contents_manager.dir_exists(self.path(
                "test")))
            self.assertTrue(self.contents_manager.dir_exists(self.path(
                "test/")))
            self.assertTrue(self.contents_manager.dir_exists(self.path(
                "test/other")))
            self.assertTrue(self.contents_manager.dir_exists(self.path(
                "test/other/")))
        finally:
            blob.delete()
        blob = bucket.blob("test/other")
        blob.upload_from_string(b"data")
        try:
            self.assertTrue(self.contents_manager.dir_exists(self.path(
                "test")))
            self.assertTrue(self.contents_manager.dir_exists(self.path(
                "test/")))
            self.assertFalse(self.contents_manager.dir_exists(self.path(
                "test/other")))
            self.assertFalse(self.contents_manager.dir_exists(self.path(
                "test/other/")))
        finally:
            blob.delete()

    def test_is_hidden(self):
        self.assertFalse(self.contents_manager.is_hidden(self.BUCKET))
        self.assertFalse(self.contents_manager.is_hidden("/" + self.BUCKET))
        self.assertFalse(self.contents_manager.is_hidden(self.BUCKET + "/"))
        self.assertFalse(self.contents_manager.is_hidden(
            "/" + self.BUCKET + "/"))
        self.assertFalse(self.contents_manager.is_hidden(self.path(
            "something")))
        self.assertTrue(self.contents_manager.is_hidden(
            self.BUCKET + "blahblah"))
        self.assertTrue(self.contents_manager.is_hidden(
            self.BUCKET + "blahblah/test"))

    def test_get(self):
        bucket = self.bucket
        blob = bucket.blob("test/other.txt")
        blob.upload_from_string(b"contents")
        try:
            model = self.contents_manager.get(self.path("test/other.txt"))
            self.assertIsInstance(model, dict)
            self.assertEqual(model["type"], "file")
            self.assertEqual(model["mimetype"], "text/plain")
            self.assertEqual(model["name"], "other.txt")
            self.assertEqual(model["path"], self.path("test/other.txt")[1:])
            self.assertEqual(model["last_modified"], blob.updated)
            self.assertIsInstance(model["last_modified"], datetime)
            self.assertEqual(model["created"], blob.updated)
            self.assertIsInstance(model["created"], datetime)
            self.assertEqual(model["content"], u"contents")
            self.assertEqual(model["format"], "text")
            self.assertEqual(model["writable"], True)
        finally:
            blob.delete()

    def test_delete_file(self):
        bucket = self.bucket
        blob = bucket.blob("test/other.txt")
        blob.upload_from_string(b"contents")
        try:
            self.contents_manager.delete_file(self.path("test/other.txt"))
            self.assertFalse(blob.exists())
        except:
            blob.delete()
            raise
        self.contents_manager.delete_file(self.BUCKET)
        self.assertFalse(bucket.exists())
        bucket.create()
        blob = bucket.blob("test/other/")
        blob.upload_from_string(b"contents")
        try:
            self.contents_manager.delete_file(self.path("test/other/"))
            self.assertFalse(blob.exists())
            self.assertFalse(bucket.blob("test/").exists())
        except:
            blob.delete()
            raise
        blob1 = bucket.blob("test/other.txt")
        blob1.upload_from_string(b"contents")
        blob2 = bucket.blob("test/next/another.txt")
        blob2.upload_from_string(b"contents")
        try:
            self.contents_manager.delete_file(self.path("test/"))
            self.assertFalse(blob1.exists())
            self.assertFalse(blob2.exists())
            self.assertFalse(bucket.blob("test/").exists())
        except:
            try:
                blob1.delete()
            finally:
                blob2.delete()
            raise

    def test_rename_file(self):
        bucket = self.bucket
        blob = bucket.blob("test/other.txt")
        blob.upload_from_string(b"contents")
        try:
            self.contents_manager.rename_file(self.path("test/other.txt"),
                                              self.path("test1/other1.txt"))
            self.assertFalse(blob.exists())
        except:
            blob.delete()
            raise
        blob = bucket.blob("test1/other1.txt")
        self.assertTrue(blob.exists())
        blob.delete()

        blob1 = bucket.blob("test/other.txt")
        blob1.upload_from_string(b"contents")
        blob2 = bucket.blob("test/other1.txt")
        blob2.upload_from_string(b"contents")
        try:
            self.contents_manager.rename_file(self.path("test/"),
                                              self.path("test1/"))
            self.assertFalse(blob1.exists())
            self.assertFalse(blob2.exists())
        except:
            try:
                blob1.delete()
            finally:
                blob2.delete()
            raise
        blob = bucket.blob("test1/other.txt")
        self.assertTrue(blob.exists())
        blob.delete()
        blob = bucket.blob("test1/other1.txt")
        self.assertTrue(blob.exists())
        blob.delete()

        blob = bucket.blob("test/dir/other.txt")
        blob.upload_from_string(b"contents")
        try:
            self.contents_manager.rename_file(self.path("test/"),
                                              self.path("test1/"))
            self.assertFalse(blob.exists())
        except:
            blob.delete()
            raise
        blob = bucket.blob("test1/dir/other.txt")
        self.assertTrue(blob.exists())
        blob.delete()

if __name__ == "__main__":
    main()
