import unittest
from unittest import mock
from tap_jira.streams import Stream

class TestOutOfRangeDate(unittest.TestCase):
    @mock.patch("tap_jira.streams.singer.utils.now", return_value="2022-05-23T09:16:11.356670Z")
    @mock.patch("tap_jira.streams.singer.write_record")
    @mock.patch("tap_jira.streams.Context")
    @mock.patch("tap_jira.streams.metadata")
    def test_out_of_range_date(self, mock_metadata, mock_Context, mock_write_record, mock_now):
        """
        Verify that the tap successfully skips record for out of range date value.
        """
        mock_records = [
            {"updated": "2017000-09-05T19:51:03.159Z", "id": "1"},  # year out of range
            {"updated": "2001-09-05T19:51:03.159000", "id": "2"},  # Correct record
            {"updated": "2001-13-05T19:51:03.159000", "id": "3"},  # month out of range
            {"updated": "2007-09-45T19:51:03.159000z", "id": "4"},  # day out of range
            {"updated": "2006-01-29T01:62:01.99999", "id": "5"},  # minutes out of range
            {"updated": "2011-09-05T19:51:03.159000", "id": "6"},  # Correct record
            {"updated": "2009-09-10T26:51:03.159000Z", "id": "7"},  # hour out of range
        ]
        mock_schema = {
            "properties": {
                "updated": {"format": "date-time", "type": ["string", "null"]},
                "id": {"type": ["string", "null"]},
            },
            "type": ["object", "null"],
        }
        mock_stream = mock_Context.get_catalog_entry.return_value
        mock_stream.schema.to_dict.return_value = mock_schema
        mock_metadata.to_map.return_value = {}  # mock_metadata

        stream_obj = Stream("stream_id", "pk_fields")
        stream_obj.write_page(mock_records)

        # Verify that only records with valid date ranges (2 records) are written
        self.assertEqual(mock_write_record.call_count, 2)

        expected_calls = [
            mock.call(
                "stream_id",
                {"updated": "2001-09-05T19:51:03.159000Z", "id": "2"},
                time_extracted="2022-05-23T09:16:11.356670Z",
            ),
            mock.call(
                "stream_id",
                {"updated": "2011-09-05T19:51:03.159000Z", "id": "6"},
                time_extracted="2022-05-23T09:16:11.356670Z",
            ),
        ]

        # Verify that the records are written with proper args
        self.assertEqual(mock_write_record.mock_calls, expected_calls)
