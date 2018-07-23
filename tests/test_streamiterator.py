from __future__ import with_statement
import pytest

import redis
from redis.exceptions import ConnectionError
from redis.client import StrictRedis

from .conftest import skip_if_server_version_lt

long_suffix = "r83waAS90OpwCKXcZpXy"
streams = ["S" + str(stream) + "_" + long_suffix for stream in range(0, 5)]
streams_from_start_dict = dict([(s, 0) for s in streams[0:3]])
first_3_streams_dict = dict([(s, 0) for s in streams])

message_num = 20


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
    last_timestamp = 0
    last_index = 0
    for msg in list_of_messages:
        timestamp = int(msg[1][0:13])
        index = int(msg[1][14:])
        assert(timestamp >= last_timestamp)
        if timestamp == last_timestamp:
            assert(index > last_index)
        last_index = index


class TestPubSubSubscribeUnsubscribe(object):

    @skip_if_server_version_lt('4.9.0')
    def test_all_stream_specifiers(self, srs):
        # Initializing without any streams should error
        with pytest.raises(redis.exceptions.RedisError):
            empty_streams = srs.streams()

        # Test that all messages from 0 to end are returned in order...
        # ...using keyword args
        messages_from_args = [msg for msg in srs.streams(block=0, stop_on_timeout=True, **first_3_streams_dict)]
        assert(len(messages_from_args) == message_num*3)
        check_response_order(messages_from_args)

        # ...using the streams keyword
        messages_from_streamdict = [msg for msg in srs.streams(streams=first_3_streams_dict, block=0,
                                                               stop_on_timeout=True)]
        assert(len(messages_from_streamdict) == message_num*3)
        check_response_order(messages_from_streamdict)

        # ...using a really short count
        messages_short_limit = [msg for msg in srs.streams(streams=first_3_streams_dict, block=0, count=3,
                                                               stop_on_timeout=True, )]
        assert(len(messages_short_limit) == message_num*3)
        check_response_order(messages_short_limit)

        # ...using a list (which will return an empty list as it is listening from now)
        messages_from_list = [msg for msg in srs.streams(streams=list(first_3_streams_dict.keys()), block=0,
                                                         stop_on_timeout=True)]
        assert(messages_from_list == [])

        # ...using a set (which will also return an empty list as it is listening from now)
        messages_from_set = [msg for msg in srs.streams(streams=set(first_3_streams_dict.keys()), block=0,
                                                        stop_on_timeout=True)]
        assert(messages_from_set == [])

        # Grab a message somewhere in the middle of the messages_from_args to get an intermediate timestamp
        middle_stream = messages_from_args[message_num-1][0]
        middle_index = messages_from_args[message_num-1][1]
        if isinstance(middle_index, bytes):
            middle_index = middle_index.decode()
        middle_ts, middle_subindex = middle_index.split("-")

        # ...using an intermediate index
        messages_from_index = [msg for msg in srs.streams(streams={middle_stream: middle_index}, block=0,
                                                          stop_on_timeout=True)]
        assert(len(messages_from_index) > 0)

        # ...using just the timestamp of the index, byte-encoded if python 3
        messages_from_timestamp = [msg for msg in srs.streams(streams={middle_stream.encode(): middle_ts.encode()},
                                                              block=0, stop_on_timeout=True)]
        assert(len(messages_from_timestamp) > 0)

    @skip_if_server_version_lt('4.9.0')
    def test_stream_blocking(self, srs):

        # Load the list, but listen at the end with no blocking.
        stream_no_block = srs.streams(streams=streams, block=None)
        with pytest.raises(StopIteration):
            _ = stream_no_block.next()

        # Listen at the end with specified blocking (10 ms) and stop_on_timeout.
        stream_with_stop = srs.streams(streams=streams, block=10, stop_on_timeout=True)
        with pytest.raises(StopIteration):
            _ = stream_with_stop.next()

        # Listen at the end with specified blocking and no stop_on_timeout.
        stream_no_stop = srs.streams(streams=streams, block=10)
        msg = stream_no_stop.next()
        assert(msg is None)

        # Add another msg to stream_no_stop and return it
        srs.xadd("S3_" + long_suffix, index=4000)
        msg = stream_no_stop.next()
        assert(isinstance(msg, tuple) and msg[0] == "S3_" + long_suffix)

        # Listen as above with specified timeout object.
        stream_timeout_resp = srs.streams(streams=streams, block=10, timeout_response="Arbitrary Response")
        msg = stream_timeout_resp.next()
        assert(msg == "Arbitrary Response")

    @skip_if_server_version_lt('4.9.0')
    def test_connection_exceptions(self, srs):

        # Default: losing connection raises connection error
        strm_raise_conn_loss = srs.streams(streams=streams, block=10)
        srs.xadd("S3_" + long_suffix, index=4000)
        _ = strm_raise_conn_loss.next()
        strm_raise_conn_loss.connection = StrictRedis(host=long_suffix)
        with pytest.raises(redis.exceptions.ConnectionError):
            _ = strm_raise_conn_loss.next()

        # returning connection error rather than raising it
        strm_ret_conn_loss = srs.streams(streams=streams, block=10, raise_connection_exceptions=False)
        srs.xadd("S3_" + long_suffix, index=4000)
        _ = strm_ret_conn_loss.next()
        real_connection = strm_ret_conn_loss.connection

        # simulate lost connection
        strm_ret_conn_loss.connection = StrictRedis(host=long_suffix)
        msg = strm_ret_conn_loss.next()
        assert isinstance(msg, ConnectionError)

        # simulate restored connection
        strm_ret_conn_loss.connection = real_connection
        msg = strm_ret_conn_loss.next()
        assert msg is None
