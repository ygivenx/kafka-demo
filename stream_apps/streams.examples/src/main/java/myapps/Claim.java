package myapps;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;



public class Claim {

    private static ProcessedClaim convertRawClaim(RawClaim rawClaim) {
        return new ProcessedClaim(rawClaim.account_id, rawClaim.name, rawClaim.claim_id, rawClaim.insured_amount);
    }

    public static void main(String[] args) {
        Properties props = new Properties();

        String BOOTSTRAP_SERVER = System.getenv().getOrDefault("KAFKA_BROKER_URL", "localhost:9092");
        String SOURCE_QUEUE = System.getenv().getOrDefault("CLAIMS_QUEUE", "queueing.claims");
        String FRAUD_DETECTION_QUEUE = System.getenv().getOrDefault("FRAUD_DETECTION_QUEUE", "streaming.fraud_detection");

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-claims");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        // Serdes
        Serde<String> stringSerde = Serdes.String();

        // RawClaim
        JsonDeserializer<RawClaim> rawClaimJsonDeserializer = new JsonDeserializer<>(RawClaim.class);
        JsonSerializer<RawClaim> rawClaimJsonSerializer = new JsonSerializer<>();
        Serde<RawClaim> rawClaimSerde = Serdes.serdeFrom(rawClaimJsonSerializer, rawClaimJsonDeserializer);

        // ProcessedClaim
        JsonDeserializer<ProcessedClaim> processedClaimJsonDeserializer = new JsonDeserializer<>(ProcessedClaim.class);
        JsonSerializer<ProcessedClaim> processedClaimJsonSerializer = new JsonSerializer<>();
        Serde<ProcessedClaim> processedClaimSerde = Serdes.serdeFrom(
                processedClaimJsonSerializer,
                processedClaimJsonDeserializer);

        // Computation Logic i.e. topology
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, RawClaim> rawClaims = builder.stream(SOURCE_QUEUE, Consumed.with(stringSerde, rawClaimSerde));
        KStream<String, ProcessedClaim> processedClaims = rawClaims.map((key, rawClaim) ->
                new KeyValue<>(rawClaim.claim_id, convertRawClaim(rawClaim)));
        processedClaims.to(FRAUD_DETECTION_QUEUE, Produced.with(stringSerde, processedClaimSerde));

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
