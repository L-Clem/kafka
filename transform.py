from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import WatermarkStrategy, Time
from pyflink.common.typeinfo import Types

env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars('file:///flink-sql-connector-kafka-1.17.2.jar')

source = KafkaSource.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_topics("currency_rate_evolution") \
    .set_group_id("bitcoin") \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(
        JsonRowDeserializationSchema.builder().type_info(Types.ROW_NAMED(
            [ 'key', 'val'], 
            [ Types.STRING(), Types.FLOAT()])).build()
    ) \
    .build()

stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "currency_rate_evolution")

stream.print()

env.execute()