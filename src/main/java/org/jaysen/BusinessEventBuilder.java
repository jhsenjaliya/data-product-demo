package org.jaysen;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.*;
import java.util.*;

public class BusinessEventBuilder {

    private static final Map<String, String> streetToZip = new HashMap<>();

    public static void main(String[] args) throws Exception {
        // Load ZIP enrichment map from GeoJSON
        loadZipCodesFromGeoJson("infra/openaddresses/source.geojson");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-domain-enrichment-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        KTable<String, JsonNode> profileTable = builder.table("user_domain.public.user_profile", Consumed.with(Serdes.String(), new JsonSerde()));
        KTable<String, JsonNode> preferencesTable = builder.table("user_domain.public.user_preferences", Consumed.with(Serdes.String(), new JsonSerde()));
        KTable<String, JsonNode> privacyTable = builder.table("user_domain.public.user_privacy", Consumed.with(Serdes.String(), new JsonSerde()));
        KTable<String, JsonNode> complianceTable = builder.table("user_domain.public.user_compliance", Consumed.with(Serdes.String(), new JsonSerde()));

        // Enrich address with ZIP
        KTable<String, JsonNode> enrichedProfile = profileTable.mapValues(profile -> {
            ObjectNode obj = (ObjectNode) profile.deepCopy();
            if (obj.has("current_address")) {
                String address = obj.get("current_address").asText();
                String zip = enrichZipFromAddress(address);
                obj.put("zipcode", zip);
            }
            return obj;
        });

        KTable<String, JsonNode> joined = enrichedProfile
                .leftJoin(preferencesTable, (profile, prefs) -> merge(profile, prefs, "preferences"))
                .leftJoin(privacyTable, (combined, privacy) -> merge(combined, privacy, "privacy"))
                .leftJoin(complianceTable, (combined, compliance) -> merge(combined, compliance, "compliance"));

        joined.toStream().to("user_domain.business_event", Produced.with(Serdes.String(), new JsonSerde()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void loadZipCodesFromGeoJson(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        InputStream in = BusinessEventBuilder.class.getClassLoader().getResourceAsStream(filePath);
        JsonNode root = mapper.readTree(in);
        for (JsonNode feature : root.withArray("features")) {
            JsonNode props = feature.get("properties");
            if (props == null) continue;
            String street = Optional.ofNullable(props.get("STREET")).map(JsonNode::asText).orElse("").toLowerCase().trim();
            String city = Optional.ofNullable(props.get("CITY")).map(JsonNode::asText).orElse("").toLowerCase().trim();
            String zip = Optional.ofNullable(props.get("POSTCODE")).map(JsonNode::asText).orElse("").trim();

            if (city.equals("san jose") && !street.isEmpty() && !zip.isEmpty()) {
                streetToZip.putIfAbsent(street, zip);
            }
        }
    }

    private static String enrichZipFromAddress(String address) {
        String addr = address.toLowerCase();
        if (!addr.contains("san jose")) return "00000";

        for (Map.Entry<String, String> entry : streetToZip.entrySet()) {
            if (addr.contains(entry.getKey())) {
                return entry.getValue();
            }
        }
        return "00000";
    }

    private static JsonNode merge(JsonNode base, JsonNode addition, String fieldName) {
        ObjectNode result = base.deepCopy();
        if (addition != null && !addition.isNull()) {
            result.set(fieldName, addition);
        }
        return result;
    }

    // JsonSerde implementation or import from Confluent / your SerDe registry
    public static class JsonSerde extends Serdes.WrapperSerde<JsonNode> {
        public JsonSerde() {
            super(new JsonSerializer(), new JsonDeserializer());
        }
    }

    public static class JsonSerializer implements org.apache.kafka.common.serialization.Serializer<JsonNode> {
        private final ObjectMapper mapper = new ObjectMapper();
        public byte[] serialize(String topic, JsonNode data) {
            try { return mapper.writeValueAsBytes(data); }
            catch (Exception e) { throw new RuntimeException(e); }
        }
    }

    public static class JsonDeserializer implements org.apache.kafka.common.serialization.Deserializer<JsonNode> {
        private final ObjectMapper mapper = new ObjectMapper();
        public JsonNode deserialize(String topic, byte[] data) {
            try { return mapper.readTree(data); }
            catch (Exception e) { throw new RuntimeException(e); }
        }
    }
}





