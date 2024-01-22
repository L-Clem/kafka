from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import WatermarkStrategy, Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import AggregateFunction

class CalculateDifferenceAggregate(AggregateFunction):
    def create_accumulator(self):
        return (None, None)

    def add(self, value, accumulator):
        return (accumulator[1], value[2])

    def get_result(self, accumulator):
        if accumulator[0] is not None:
            return accumulator[1] - accumulator[0]
        else:
            return 0.0  

    def merge(self, a, b):
        return (a[0], b[1])


env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars("file:///tmp/flink-sql-connector-kafka-1.17.2.jar")
print(env)

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

stream.map(lambda ligne : (ligne[0], f"{ligne[1]}", ligne[1]), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) \
    .key_by(lambda ligne : ligne[0]) \
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) \
    .aggregate(CalculateDifferenceAggregate(), output_type=Types.FLOAT()) \
    .print()

env.execute()
