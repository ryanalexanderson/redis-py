
class Stream(object):
    def __init__(self, redis_conn, count=100, block=None, timeout_response=None, **kwargs):
        self.BIG_NUMBER = 99999999999999
        self.block = block if block else 1
        self.topic_hit_limit = set()
        self.streams = kwargs
        self.redis_conn = redis_conn.get_connection if isinstance(red)
        self.count = count
        self.timeout_response = timeout_response
        self.buffer_dict = self.redis_conn.xread(self.count, None, **self.streams)
        self.update_last_and_limit()

    def update_last_and_limit(self):
        self.topic_hit_limit = set()
        if self.buffer_dict is not None:
            for stream_name, record_list in self.buffer_dict.items():
                if len(record_list):  # always yes?
                    self.streams[stream_name] = record_list[-1][0]
                if len(record_list) == self.count:
                    self.topic_hit_limit.add(stream_name)

    def get_lowest(self):
        lowest_timestamp_str = self.BIG_NUMBER
        lowest_index = 9999
        lowest_stream = None
        for stream_name, record_list in self.buffer_dict.items():
            if len(record_list):
                if stream_name in self.topic_hit_limit and len(record_list)==0:
                    temp_dict = self.redis_conn.xread(self.count, self.block, **{stream_name: self.streams[stream_name]})
                    self.buffer_dict[stream_name] = temp_dict[stream_name]
                    self.update_last_and_limit()

                if record_list[0][0][0:13] < lowest_timestamp_str:
                    lowest_timestamp_str = record_list[0][0][0:13]
                    lowest_index = int(record_list[0][0][14:])
                    lowest_stream = stream_name
                elif record_list[0][0][0:13] == lowest_timestamp_str:
                    lowest_index = int(record_list[0][0][14:])
                    lowest_stream = stream_name
        return lowest_timestamp_str, lowest_index, lowest_stream

    def __iter__(self):
        return self

    def __next__(self):
        if self.buffer_dict is None or not any(self.buffer_dict.values()):
            self.buffer_dict = self.redis_conn.xread(self.count, self.block, **self.streams)
            if self.buffer_dict is None:
                return self.timeout_response
            self.update_last_and_limit()

        (lowest_timestamp_str, lowest_index, lowest_stream) = self.get_lowest()
        if lowest_timestamp_str < self.BIG_NUMBER:
            entry = self.buffer_dict[lowest_stream].pop(0)
            return lowest_stream, entry[0], entry[1]

    next = __next__  # Python 2 compatibility

