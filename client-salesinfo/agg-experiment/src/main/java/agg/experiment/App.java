package agg.experiment;

// Needed for settings
import java.util.Properties;

// Needed for avro schema and schema registry
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

// Needed for serialization/deserialization
import org.apache.kafka.common.serialization.Serdes;

// important import! No google import
import org.apache.kafka.common.utils.Bytes;

// Kafka Streams objects
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

public class App {


    public static void main(String[] args) {

        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "agg-example");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        settings.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");

        final StreamsBuilder builder = new StreamsBuilder();
        /*
        Create sales table using default serdes defined elsewhere.
        Key: salesID String, value: SalesInfo with region and amount.
        */
        KTable<String, SalesInfo> sales = builder.table("sales-topic");
        // Group the sales table by region
        KGroupedTable<String, Integer> groupedTable = sales
        .groupBy(
            (saleID, saleInfo) -> KeyValue.pair(saleInfo.region, saleInfo.amount),
            Grouped.with(Serdes.String(), Serdes.Integer())
        );
        // Aggregate value of the groupedTable, which is sales amount.
        KTable<String, Integer> aggregated = groupedTable.aggregate(
            () -> 0, /* initializer */
            (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
            (aggKey, oldValue, aggValue) -> aggValue - oldValue, /* subtractor */
            Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>
                as("aggregated-table-store" /* state store name */)
                .withKeySerde(Serdes.String()) /* key serde */
                .withValueSerde(Serdes.Integer())); /* serde for aggregate value */
        
        // Produce to kafka
        aggregated.toStream().to("sales-by-region");

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, settings);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
        }));

    }

    
}
