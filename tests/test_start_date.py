"""
Test that the start_date configuration is respected
"""
from dateutil.parser import parse
from datetime import datetime as dt
from datetime import timedelta

from tap_tester import menagerie, runner
from tap_tester.logger import LOGGER

from base import BaseTapTest


class StartDateTest(BaseTapTest):
    """
    Test that the start_date configuration is respected

    • verify that a sync with a later start date has at least one record synced
      and less records than the 1st sync with a previous start date
    • verify that each stream has less records than the earlier start date sync
    • verify all data from later start data has bookmark values >= start_date
    • verify that the minimum bookmark sent to the target for the later start_date sync
      is greater than or equal to the start date

    """

    @staticmethod
    def name():
        return "tt_jira_start_date_test"

    def streams_under_test(self):
        incremental_streams = {
            stream
            for stream, method in self.expected_replication_method().items()
            if method is self.INCREMENTAL
        }
        child_streams = self.child_streams()

        return incremental_streams - child_streams

    def timedelta_formatted(self, dtime, days, str_format):
        date_stripped = dt.strptime(dtime, str_format)
        return_date = date_stripped + timedelta(days=days)

        return dt.strftime(return_date, str_format)

    def test_run(self):
        """Test we get a lot of data back based on the start date configured in base"""

        streams_under_test = self.streams_under_test()

        conn_id = self.create_connection_with_initial_discovery()

        # Select streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in streams_under_test]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # get results
        first_sync_records = runner.get_records_from_target_output()
        state = menagerie.get_state(conn_id)

        # set the start date for a new connection based off state
        bookmarked_values = []
        expected_replication_keys_by_stream = self.expected_replication_keys()
        for stream in streams_under_test:
            replication_key = list(expected_replication_keys_by_stream[stream])[0]
            bookmarked_values.append(state['bookmarks'][stream][replication_key])

        # grab the minimum bookmark from state to ensure we fetch data from all sync2 streams
        minium_bookmark_value = sorted(bookmarked_values)[0].split("T")[0]
        start_date = self.timedelta_formatted(minium_bookmark_value, days=0, str_format="%Y-%m-%d")
        self.start_date = start_date + "T00:00:00Z"

        # create a new connection with the new  more recent start_date
        conn_id = self.create_connection_with_initial_discovery(original_properties=False)

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in streams_under_test]
        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator with a more recent start date
        second_sync_record_count = self.run_sync(conn_id)

        # get results
        second_sync_records = runner.get_records_from_target_output()

        for stream in streams_under_test:
            with self.subTest(stream=stream):

                # gather expectations
                replication_key = list(expected_replication_keys_by_stream[stream])[0]

                # get results
                record_messages = [message['data']
                                   for message in second_sync_records[stream]['messages']
                                   if message.get('action') == 'upsert']
                if stream == 'issues':
                    replication_key_values = [record_message['fields'][replication_key] for record_message in record_messages]
                else:
                    replication_key_values = [record_message[replication_key] for record_message in record_messages]
                max_replication_key_value = sorted(replication_key_values)[-1]

                # verify that each stream has less records than the first connection sync
                self.assertGreater(
                    first_sync_record_count.get(stream, 0),
                    second_sync_record_count.get(stream, 0),
                    msg="second had more records, start_date usage not verified",
                    logging="verify less records are replicated with a more recent start date"
                )

                # verify all data from 2nd sync >= start_date
                self.assertGreaterEqual(
                    parse(max_replication_key_value), parse(self.start_date),
                    logging="verify on second sync no records are replicated prior to the start date"
                )
