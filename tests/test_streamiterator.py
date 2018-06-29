from __future__ import with_statement
import pytest
import time

import redis
from redis.exceptions import ConnectionError
from redis._compat import basestring, u, unichr, b

from .conftest import sr
from .conftest import skip_if_server_version_lt

long_suffix = "r83waAS90OpwCKXcZpXy"
streams = ["S" + str(stream) + "_"  + long_suffix for stream in range(0,5)]
streams_from_start_dict = dict([(s,0) for s in streams])
message_num=20

@pytest.fixture
def srs(sr):
    sr.delete(*streams)
    for x in range(0, message_num):
        sr.xadd("S0_"+long_suffix, index=x)
        sr.xadd("S1_"+long_suffix, index=1000 + x)
        sr.xadd("S2_"+long_suffix, index=2000 + x)
    yield sr
    sr.delete(*streams)


def check_response_order(list_of_messages):
    in_order=True
    last_timestamp = 0
    last_index = 0
    for msg in list_of_messages:
        timestamp = int(msg[1][0:13])
        index = int(msg[1][14:])
        assert(timestamp >= last_timestamp)
        if timestamp == last_timestamp:
            assert(index > last_index)
        last_ts = timestamp
        last_index = index


class TestPubSubSubscribeUnsubscribe(object):

    @skip_if_server_version_lt('4.9.0')
    def test_all_stream_specifiers(self, srs):
        # Test that all messages from 0 to end are returned in order...

        # ...using keyword args
        messages_from_args = [msg for msg in srs.streams(**streams_from_start_dict)]
        assert(len(messages_from_args) == message_num*3)
        check_response_order(messages_from_args)

        # ...using the streams keyword
        messages_from_streamdict = [msg for msg in srs.streams(streams=streams_from_start_dict)]
        assert(len(messages_from_streamdict) == message_num*3)
        check_response_order(messages_from_streamdict)

        # ...using a list (which will return an empty list as it is listening from now)
        messages_from_list = [msg for msg in srs.streams(streams=streams)]
        assert(messages_from_list == [])

        # ...using a set (which will also return an empty list as it is listening from now)
        messages_from_set = [msg for msg in srs.streams(streams=set(streams_from_start_dict))]
        assert(messages_from_set == [])


    @skip_if_server_version_lt('4.9.0')
    def test_stream_blocking(self, srs):

        # Load the list, but listen at the end with no blocking.
        stream_no_block = srs.streams(streams=streams, block=None)
        with pytest.raises(StopIteration):
            msg = stream_no_block.next()

        # Listen at the end with specified blocking (10 ms) and stop_on_timeout.
        stream_with_stop = srs.streams(streams=streams, block=10, stop_on_timeout=True)
        with pytest.raises(StopIteration):
            msg = stream_with_stop.next()

        # Listen at the end with specified blocking and no stop_on_timeout.
        stream_no_stop = srs.streams(streams=streams, block=10)
        msg = stream_no_stop.next()
        assert(msg is None)

        # Listen as above with specified timeout object.
        stream_timeout_resp = srs.streams(streams=streams, block=10, timeout_response="Arbitrary Response")
        msg = stream_timeout_resp.next()
        assert(msg == "Arbitrary Response")

    @skip_if_server_version_lt('4.9.0')
    def test_connection_exceptions(self, srs):

        # Load the list, but listen at the end with no blocking.
        stream_conn_loss = srs.streams(streams=streams, block=10)
        srs.xadd("S3_" + long_suffix, index=4000)
        msg = stream_conn_loss.next()

        # Listen at the end with specified blocking (10 ms) and stop_on_timeout.
        stream_with_stop = srs.streams(streams=streams, block=10, stop_on_timeout=True)
        with pytest.raises(StopIteration):
            msg = stream_with_stop.next()

        # Listen at the end with specified blocking and no stop_on_timeout.
        stream_no_stop = srs.streams(streams=streams, block=10)
        msg = stream_no_stop.next()
        assert (msg is None)

        # Listen as above with specified timeout object.
        stream_timeout_resp = srs.streams(streams=streams, block=10, timeout_response="Arbitrary Response")
        msg = stream_timeout_resp.next()
        assert (msg == "Arbitrary Response")

        messages_from_args = [msg for msg in srs.streams(**streams_from_start_dict)]
        assert(len(messages_from_args) == message_num*3)
        check_response_order(messages_from_args)

        messages_from_streamdict = [msg for msg in srs.streams(streams=streams_from_start_dict)]
        assert(len(messages_from_streamdict) == message_num*3)
        check_response_order(messages_from_streamdict)

        messages_from_list = [msg for msg in srs.streams(streams=streams)]
        assert(messages_from_list == [])

        messages_from_set = [msg for msg in srs.streams(streams=set(streams_from_start_dict))]
        assert(messages_from_set == [])





