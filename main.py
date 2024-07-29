import os
from abc import ABC
import json
from pyflink.common import WatermarkStrategy, Types, Row, SimpleStringSchema
from pyflink.datastream import DataStream, FlatMapFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema

from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment, RuntimeExecutionMode


class MyFlatMap(FlatMapFunction, ABC):
    def flat_map(self, value: str):
        obj: dict = json.loads(value)
        l = []
        for key, val in obj.items():
            l.append(f"{key} is {val}")
        return l


class SourceData(object):
    def __init__(self, env):
        self.env = env
        file_path = "file:///C:/Users/ori/PycharmProjects/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar"
        self.env.add_jars(file_path)
        self.env.add_classpaths(file_path)
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.env.set_parallelism(1)

    def get_data(self):
        source = KafkaSource.builder() \
            .set_bootstrap_servers("localhost:9092") \
            .set_topics("numtest") \
            .set_group_id("my-group") \
            .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        sink = KafkaSink.builder() \
            .set_bootstrap_servers("localhost:9092") \
            .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                   .set_topic('output')
                                   .set_value_serialization_schema(SimpleStringSchema()).build()) \
            .build()

        stream: DataStream = self.env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
        mapped = stream.flat_map(MyFlatMap(), Types.STRING())
        mapped.sink_to(sink)

        self.env.execute("source")


a = SourceData(StreamExecutionEnvironment.get_execution_environment())
a.get_data()
