package iot;

import myapps.JsonDeserializer;
import myapps.JsonSerializer;
import myapps.RawClaim;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.kstream.Consumed.as;

class Tuple2<T1, T2> {
    public T1 value1;
    public T2 value2;

    Tuple2(T1 v1, T2 v2) {
        value1 = v1;
        value2 = v2;
    }
}

public class IoTStreamApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        String BOOTSTRAP_SERVER = System.getenv().
                getOrDefault("KAFKA_BROKER_URL",
                        "localhost:9092");

        String SOURCE_QUEUE = System.getenv().getOrDefault("TELEMATICS_QUEUE",
                "streaming.driving.speed");
        String HIGHSPEED_QUEUE = System.getenv().getOrDefault("HIGHSPEED_QUEUE",
                "streaming.highspeed.drivers");

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-telematics");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());

        Serde<String> stringSerde = Serdes.String();
        JsonDeserializer<Tuple2> tuple2JsonDeserializer = new JsonDeserializer<>(Tuple2.class);
        JsonSerializer<Tuple2> tuple2JsonSerializer = new JsonSerializer<>();
        Serde<Tuple2> tuple2Serdes = Serdes.serdeFrom(tuple2JsonSerializer, tuple2JsonDeserializer);

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Double> speed_stream = builder.stream(SOURCE_QUEUE);

        final KTable<String, Tuple2<Double, Double>> countAndSum = speed_stream
                .groupByKey()
                .aggregate(
                        new Initializer<Tuple2<Double, Double>>() {
                            @Override
                            public Tuple2<Double, Double> apply() {
                                return new Tuple2<>(0.0, 0.0);
                            }
                        },
                        new Aggregator<String, Double, Tuple2<Double, Double>>() {
                            @Override
                            public Tuple2<Double, Double> apply(String key, Double value, Tuple2<Double, Double> aggregate) {
                                ++aggregate.value1;
                                aggregate.value2 += value;
                                return aggregate;
                            }
                        }

                );

        final KTable<String, Double> average = countAndSum.mapValues(
                new ValueMapper<Tuple2<Double, Double>, Double>() {
                    @Override
                    public Double apply(Tuple2<Double, Double> value) {
                        return value.value2/ (double) value.value1;
                    }
                }
        );

        average.toStream().to(HIGHSPEED_QUEUE, Produced.with(stringSerde, Serdes.Double()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }

        System.exit(0);
    }
}
