import csv
import io
import json
import tempfile
import unittest
from pathlib import Path

import daily_pump


class MissingObject(Exception):
    response = {"Error": {"Code": "404"}}


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.uploads = 0

    def head_object(self, Bucket, Key):
        try:
            return self.objects[(Bucket, Key)]
        except KeyError:
            raise MissingObject() from None

    def upload_file(self, Filename, Bucket, Key, ExtraArgs):
        self.uploads += 1
        self.objects[(Bucket, Key)] = {
            "Metadata": ExtraArgs["Metadata"],
            "Body": Path(Filename).read_bytes(),
        }

    def get_object(self, Bucket, Key):
        try:
            obj = self.objects[(Bucket, Key)]
        except KeyError:
            raise MissingObject() from None
        return {"Body": io.BytesIO(obj["Body"])}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[(Bucket, Key)] = {"Body": Body}


class DailyPumpTests(unittest.TestCase):
    def test_extract_batch_counts_multiline_csv_records(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.csv"
            batch = Path(directory) / "batch.csv"
            with source.open("w", encoding="utf-8", newline="") as handle:
                handle.write('id,note\n1,"first\nrecord"\n2,second\n3,third\n')

            self.assertEqual(daily_pump.extract_batch(source, batch, 0, 2), (2, False))
            with batch.open(newline="", encoding="utf-8") as handle:
                self.assertEqual(list(csv.reader(handle)), [
                    ["id", "note"], ["1", "first\nrecord"], ["2", "second"]
                ])

            self.assertEqual(daily_pump.extract_batch(source, batch, 2, 2), (1, True))
            with batch.open(newline="", encoding="utf-8") as handle:
                self.assertEqual(list(csv.reader(handle)), [["id", "note"], ["3", "third"]])

    def test_retry_uses_existing_object_then_updates_state(self):
        s3 = FakeS3()
        metadata = {"source-sha256": "a" * 64, "row-start": "1", "row-end": "2"}
        with tempfile.TemporaryDirectory() as directory:
            batch = Path(directory) / "batch.csv"
            batch.write_text("id\n1\n2\n", encoding="utf-8")
            daily_pump.upload_batch(s3, "raw-zone", "batch.csv", batch, metadata)
            self.assertIsNone(daily_pump.read_state(s3, "system-state"))

            daily_pump.upload_batch(s3, "raw-zone", "batch.csv", batch, metadata)
            self.assertEqual(s3.uploads, 1)
            daily_pump.write_state(s3, "system-state", 2, "a" * 64, "source.csv", False)
            state = daily_pump.read_state(s3, "system-state")
            self.assertEqual(state["last_row"], 2)
            self.assertEqual(state["source_sha256"], "a" * 64)

            with self.assertRaises(ValueError):
                daily_pump.upload_batch(
                    s3, "raw-zone", "batch.csv", batch,
                    {**metadata, "row-end": "3"}
                )

    def test_invalid_state_fails_instead_of_restarting(self):
        s3 = FakeS3()
        s3.objects[("system-state", daily_pump.STATE_KEY)] = {
            "Body": json.dumps({"last_row": -1}).encode("utf-8")
        }
        with self.assertRaises(ValueError):
            daily_pump.read_state(s3, "system-state")


if __name__ == "__main__":
    unittest.main()
