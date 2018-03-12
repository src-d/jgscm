import base64
from datetime import datetime
import pickle
from unittest import main, TestCase
import uuid
import sys

import nbformat
from tornado import web

from jgscm import GoogleStorageContentManager

if sys.version_info[0] == 2:
    import socket
    BrokenPipeError = socket.error
    base64.encodebytes = base64.encodestring
    base64.decodebytes = base64.decodestring
else:
    unicode = str


class TestGoogleStorageContentManager(TestCase):
    BUCKET = "%s-%s" % ("jgcsm", uuid.uuid4())
    NOTEBOOK = """{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {
    "collapsed": false
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Populating the interactive namespace from numpy and matplotlib\\n"
     ]
    }
   ],
   "source": [
    "%pylab inline"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.5.1+"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}"""

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
        self.assertFalse(self.contents_manager.file_exists(self.BUCKET))
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

        self.contents_manager.hide_dotted_blobs = True
        self.assertTrue(self.contents_manager.is_hidden(self.path(
            ".test")))
        self.assertTrue(self.contents_manager.is_hidden(self.path(
            ".test/")))
        self.contents_manager.hide_dotted_blobs = False
        self.assertFalse(self.contents_manager.is_hidden(self.path(
            ".test")))
        self.assertFalse(self.contents_manager.is_hidden(self.path(
            ".test/")))
        self.contents_manager.hide_dotted_blobs = True
        self.assertFalse(self.contents_manager.is_hidden(self.path(
            ".test/other")))
        self.contents_manager.hide_dotted_blobs = False
        self.assertFalse(self.contents_manager.is_hidden(self.path(
            ".test/other")))
        self.contents_manager.hide_dotted_blobs = True

    def test_get(self):
        model = self.contents_manager.get("/")
        self.assertEqual(model["type"], "directory")
        self.assertEqual(model["mimetype"], "application/x-directory")
        self.assertEqual(model["name"], "")
        self.assertEqual(model["path"], "")
        self.assertEqual(model["last_modified"], "")
        self.assertEqual(model["created"], "")
        self.assertEqual(model["format"], "json")
        self.assertEqual(model["writable"], True)
        self.assertIsInstance(model["content"], list)
        self.assertGreaterEqual(len(model["content"]), 1)
        for m in model["content"]:
            self.assertEqual(m["type"], "directory")
            self.assertEqual(m["mimetype"], "application/x-directory")
            if m["name"] == self.BUCKET:
                self.assertEqual(m["path"], self.BUCKET)
                self.assertEqual(m["last_modified"], "")
                self.assertEqual(m["created"], "")
                self.assertIsNone(m["format"])
                self.assertIsNone(m["content"])
                self.assertEqual(m["writable"], True)

        model = self.contents_manager.get(self.path(""))
        self.assertEqual(model["type"], "directory")
        self.assertEqual(model["mimetype"], "application/x-directory")
        self.assertEqual(model["name"], self.BUCKET)
        self.assertEqual(model["path"], self.BUCKET)
        self.assertEqual(model["last_modified"], "")
        self.assertEqual(model["created"], "")
        self.assertEqual(model["format"], "json")
        self.assertEqual(model["content"], [])
        self.assertEqual(model["writable"], True)

        model2 = self.contents_manager.get(self.path(""), type="directory")
        self.assertEqual(model, model2)
        with self.assertRaises(web.HTTPError):
            self.contents_manager.get(self.path(""), type="file")
        with self.assertRaises(web.HTTPError):
            self.contents_manager.get(self.path(""), type="notebook")

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

            model2 = self.contents_manager.get(self.path("test/other.txt"),
                                               type="file")
            self.assertEqual(model, model2)
            with self.assertRaises(web.HTTPError):
                self.contents_manager.get(self.path("test/other.txt"),
                                          type="directory")
            with self.assertRaises(nbformat.reader.NotJSONError):
                self.contents_manager.get(self.path("test/other.txt"),
                                          type="notebook")
        except:  # nopep8
            blob.delete()
            raise

        model = self.contents_manager.get(self.path(""))
        self.assertEqual(model["type"], "directory")
        self.assertEqual(model["mimetype"], "application/x-directory")
        self.assertEqual(model["name"], self.BUCKET)
        self.assertEqual(model["path"], self.BUCKET)
        self.assertEqual(model["last_modified"], "")
        self.assertEqual(model["created"], "")
        self.assertEqual(model["format"], "json")
        self.assertEqual(model["writable"], True)
        self.assertIsInstance(model["content"], list)
        self.assertEqual(len(model["content"]), 1)
        model = model["content"][0]
        self.assertEqual(model["type"], "directory")
        self.assertEqual(model["mimetype"], "application/x-directory")
        self.assertEqual(model["name"], "test")
        self.assertEqual(model["path"], self.path("test")[1:])
        self.assertEqual(model["last_modified"], "")
        self.assertEqual(model["created"], "")
        self.assertIsNone(model["content"])
        self.assertIsNone(model["format"])
        self.assertEqual(model["writable"], True)

        blob2 = bucket.blob("test/fold/another.txt")
        blob2.upload_from_string(b"contents")
        try:
            model = self.contents_manager.get(self.path("test/"))
            self.assertIsInstance(model, dict)
            self.assertEqual(model["type"], "directory")
            self.assertEqual(model["mimetype"], "application/x-directory")
            self.assertEqual(model["name"], "test")
            self.assertEqual(model["path"], self.path("test")[1:])
            self.assertEqual(model["last_modified"], "")
            self.assertEqual(model["created"], "")
            self.assertEqual(model["format"], "json")
            self.assertEqual(model["writable"], True)
            self.assertIsInstance(model["content"], list)
            self.assertEqual(len(model["content"]), 2)
            fc, dc = model["content"]
        finally:
            blob.delete()
            blob2.delete()
        self.assertIsInstance(fc, dict)
        self.assertEqual(fc["type"], "file")
        self.assertEqual(fc["mimetype"], "text/plain")
        self.assertEqual(fc["name"], "other.txt")
        self.assertEqual(fc["path"], self.path("test/other.txt")[1:])
        self.assertIsNone(fc["content"])
        self.assertIsNone(fc["format"])
        self.assertEqual(fc["last_modified"], blob.updated)
        self.assertIsInstance(fc["last_modified"], datetime)
        self.assertEqual(fc["created"], blob.updated)
        self.assertIsInstance(fc["created"], datetime)

        self.assertIsInstance(dc, dict)
        self.assertEqual(dc["type"], "directory")
        self.assertEqual(dc["mimetype"], "application/x-directory")
        self.assertEqual(dc["name"], "fold")
        self.assertEqual(dc["path"], self.path("test/fold")[1:])
        self.assertIsNone(dc["content"])
        self.assertIsNone(dc["format"])
        self.assertEqual(dc["last_modified"], "")
        self.assertEqual(dc["created"], "")

    def test_get_base64(self):
        bucket = self.bucket
        blob = bucket.blob("test.pickle")
        obj = {"one": 1, "two": [2, 3]}
        blob.upload_from_string(pickle.dumps(obj))
        model = self.contents_manager.get(
            self.path("test.pickle"), format="base64")
        self.assertEqual(model["type"], "file")
        self.assertEqual(model["mimetype"], "application/octet-stream")
        self.assertEqual(model["format"], "base64")
        content = model["content"]
        self.assertIsInstance(content, unicode)
        bd = base64.decodebytes(content.encode())
        self.assertEqual(obj, pickle.loads(bd))

    def test_get_notebook(self):
        bucket = self.bucket
        blob = bucket.blob("test.ipynb")
        blob.upload_from_string(self.NOTEBOOK.encode())
        try:
            model = self.contents_manager.get(
                self.path("test.ipynb"), type="notebook")
            self.assertEqual(model["type"], "notebook")
            self.assertEqual(model["mimetype"], "application/x-ipynb+json")
            self.assertEqual(model["format"], "json")
            self.assertIsInstance(model["content"],
                                  nbformat.notebooknode.NotebookNode)
        finally:
            blob.delete()

    def test_delete_file(self):
        bucket = self.bucket
        blob = bucket.blob("test/other.txt")
        blob.upload_from_string(b"contents")
        try:
            self.contents_manager.delete_file(self.path("test/other.txt"))
            self.assertFalse(blob.exists())
        except:  # nopep8
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
        except:  # nopep8
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
        except:  # nopep8
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
        except:  # nopep8
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
        except:  # nopep8
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
        except:  # nopep8
            blob.delete()
            raise
        blob = bucket.blob("test1/dir/other.txt")
        self.assertTrue(blob.exists())

        new_bucket = self.contents_manager.client.bucket(
            "jgscm-%s-new" % uuid.uuid4())
        new_bucket.create()
        try:
            self.contents_manager.rename_file(
                self.path("test1/"), new_bucket.name + "/" + "test/")
            self.assertFalse(blob.exists())
        except:  # nopep8
            blob.delete()
            new_bucket.delete(force=True)
            raise
        try:
            self.assertTrue(new_bucket.blob("test/dir/other.txt").exists())
        finally:
            new_bucket.delete(force=True)

    def test_save_dir(self):
        self.contents_manager.save({
            "type": "directory"
        }, self.path("test/"))
        bucket = self.bucket
        blob = bucket.blob("test/")
        self.assertTrue(blob.exists())
        blob.delete()

    def test_save_file(self):
        self.contents_manager.save({
            "type": "file",
            "content": "blah-blah-blah",
            "format": "text"
        }, self.path("test.txt"))
        bucket = self.bucket
        blob = bucket.blob("test.txt")
        self.assertTrue(blob.exists())
        try:
            self.assertEqual(blob.download_as_string(), b"blah-blah-blah")
        finally:
            blob.delete()

        obj = {"one": 1, "two": [2, 3]}
        self.contents_manager.save({
            "type": "file",
            "content": base64.encodebytes(pickle.dumps(obj)).decode("ascii"),
            "format": "base64"
        }, self.path("test.pickle"))
        bucket = self.bucket
        blob = bucket.blob("test.pickle")
        self.assertTrue(blob.exists())
        try:
            self.assertEqual(blob.download_as_string(), pickle.dumps(obj))
        finally:
            blob.delete()

    def test_save_notebook(self):
        nb = nbformat.reads(self.NOTEBOOK, 4)
        self.contents_manager.save({
            "type": "notebook",
            "content": nb
        }, self.path("test.ipynb"))
        bucket = self.bucket
        blob = bucket.blob("test.ipynb")
        self.assertTrue(blob.exists())
        try:
            self.assertEqual(blob.download_as_string(), self.NOTEBOOK.encode())
        finally:
            blob.delete()


if __name__ == "__main__":
    main()
