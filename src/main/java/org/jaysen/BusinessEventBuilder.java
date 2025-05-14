package org.jaysen;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class BusinessEventBuilder {

    private static final Map<String, String> streetToZip = new HashMap<>();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // Load ZIP enrichment map from GeoJSON
        loadZipCodesFromGeoJson("infra/openaddresses/source.geojson");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-business-events-builder-13");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "checkpoints/kafka/kstreams-state");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

//        KTable<String, JsonNode> profileTable = builder.table("user_domain.public.user_profile", Consumed.with(Serdes.String(), new JsonSerde()));
//        KTable<String, JsonNode> preferencesTable = builder.table("user_domain.public.user_preferences", Consumed.with(Serdes.String(), new JsonSerde()));
//        KTable<String, JsonNode> privacyTable = builder.table("user_domain.public.user_privacy", Consumed.with(Serdes.String(), new JsonSerde()));
//        KTable<String, JsonNode> complianceTable = builder.table("user_domain.public.user_compliance", Consumed.with(Serdes.String(), new JsonSerde()));

        KTable<String, JsonNode> profileTable = builder.table("user_domain.public.user_profile", Consumed.with(Serdes.String(), new JsonSerde()).withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));
        KTable<String, JsonNode> preferencesTable = builder.table("user_domain.public.user_preferences", Consumed.with(Serdes.String(), new JsonSerde()).withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));
        KTable<String, JsonNode> privacyTable = builder.table("user_domain.public.user_privacy", Consumed.with(Serdes.String(), new JsonSerde()).withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));
        KTable<String, JsonNode> complianceTable = builder.table("user_domain.public.user_compliance", Consumed.with(Serdes.String(), new JsonSerde()).withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));

//        KTable<String, JsonNode> profileTable = rawProfileTable
//                .toStream()
//                .selectKey((k, v) -> String.valueOf(v.get("id").asLong()))
//                .toTable();

//        profileTable.toStream().print(Printed.<String, JsonNode>toSysOut().withLabel("profile"));

        // Enrich address with ZIP
        KTable<String, JsonNode> enrichedProfile = profileTable.mapValues(profile -> {
            ObjectNode obj = (ObjectNode) profile.deepCopy();
            // Rename "id" to "user_id"
            if (obj.has("id")) {
                obj.put("user_id", obj.get("id").asInt());
                obj.remove("id");
            }
            if (obj.has("profile") ) {
                try {
                    ObjectNode profileNode = (ObjectNode) mapper.readTree(obj.get("profile").asText(""));
                    String address = profileNode.get("address").asText();
                    String zip = enrichZipFromAddress(address);
                    profileNode.put("zipcode", zip);
                    obj.set("profile", profileNode);
//                    obj.put();
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
            return obj;
        });

        KTable<String, JsonNode> enrichedPrivacy = privacyTable.mapValues(privacy -> {
            ObjectNode obj = (ObjectNode) privacy.deepCopy();
            // Call external policy API to fetch consent flags
            try {
                ObjectNode privacyNode = (ObjectNode) mapper.readTree(obj.get("privacy").asText(""));

                JsonNode policyResponse = fetchPolicyConsent(obj.path("user_id").asText());
                if (policyResponse != null && policyResponse.has("doNotSellMyDataConsent")) {
                    privacyNode.set("doNotSellMyDataConsent", policyResponse.get("doNotSellMyDataConsent"));
                }
                obj.set("privacy", privacyNode);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return obj;
        });

//        enrichedProfile.toStream().print(Printed.<String, JsonNode>toSysOut().withLabel("profile"));
//        enrichedPrivacy.toStream().print(Printed.<String, JsonNode>toSysOut().withLabel("privacy"));

        KTable<String, JsonNode> joined = enrichedProfile
                .leftJoin(preferencesTable, (profile, prefs) -> merge(profile, prefs, "preferences"))
                .leftJoin(enrichedPrivacy, (combined, privacy) -> merge(combined, privacy, "privacy"))
                .leftJoin(complianceTable, (combined, compliance) -> merge(combined, compliance, "compliance"));

        joined.toStream().to("user_domain.business_event", Produced.with(Serdes.String(), new JsonSerde()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void loadZipCodesFromGeoJson(String filePath) throws IOException {
//        InputStream in = BusinessEventBuilder.class.getClassLoader().getResourceAsStream(filePath);
        URL resource = BusinessEventBuilder.class.getClassLoader().getResource(filePath);
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(resource.toURI())))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Process each line
                JsonNode root = mapper.readTree(line);
                JsonNode props = root.get("properties");
                if (props == null) continue;
                String number = Optional.ofNullable(props.get("number")).map(JsonNode::asText).orElse("").toLowerCase().trim();
                String street = Optional.ofNullable(props.get("street")).map(JsonNode::asText).orElse("").toLowerCase().trim();
                String city = Optional.ofNullable(props.get("city")).map(JsonNode::asText).orElse("").toLowerCase().trim();
                String zip = Optional.ofNullable(props.get("postcode")).map(JsonNode::asText).orElse("").trim();

                String addressStr = String.format("%s %s", number, street);

//                if (city.equals("san jose") && !street.isEmpty() && !zip.isEmpty()) {
                    streetToZip.putIfAbsent(addressStr, zip);   // TODO: should be lazy cache
//                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

//        JsonNode root = mapper.readTree(in);
//        for (JsonNode props : root.get("properties")) {
////            JsonNode props = feature.get("properties");
//            if (props == null) continue;
//            String street = Optional.ofNullable(props.get("street")).map(JsonNode::asText).orElse("").toLowerCase().trim();
//            String city = Optional.ofNullable(props.get("city")).map(JsonNode::asText).orElse("").toLowerCase().trim();
//            String zip = Optional.ofNullable(props.get("postcode")).map(JsonNode::asText).orElse("").trim();
//
//            if (city.equals("san jose") && !street.isEmpty() && !zip.isEmpty()) {
//                streetToZip.putIfAbsent(street, zip);   // TODO: should be lazy cache
//            }
//        }
    }

    private static String enrichZipFromAddress(String address) {
        String addr = address.toLowerCase().replace(",", "");
        if (!addr.contains("san jose"))
            return "00000"; // TODO: currently limited to san jose to reduce mem & download size for the workshop

        for (Map.Entry<String, String> entry : streetToZip.entrySet()) {
            if (addr.contains(entry.getKey()) || entry.getKey().contains(addr)) {
                return entry.getValue();
            }
        }
        return "00000"; // TODO: this is actually bad quality, value can be null instead
    }

//    private static JsonNode merge(JsonNode base, JsonNode addition, String fieldName) {
//        ObjectNode result = base.deepCopy();
//        if (addition != null && !addition.isNull()) {
//            result.set(fieldName, addition);
//        }
//        return result;
//    }

    // flattens 'privacy', 'preferences', or 'compliance' if nested string field exists
    private static JsonNode merge(JsonNode base, JsonNode addition, String fieldName) {
        ObjectNode result = base.deepCopy();
//        System.out.println("addition: " + fieldName + ", base:" + base + ", fieldName:" + fieldName);

        if (addition != null && !addition.isNull()) {
            // upstream data issue resolved by fetching nested entity and structuring to json than a text string from event data.
            if (addition.has(fieldName)) {
                try {
                    if(addition.get(fieldName).isTextual()) {
                        result.set(fieldName, mapper.readTree(addition.get(fieldName).asText("")));
                    }else{
                        result.set(fieldName, addition.get(fieldName));
                    }
                } catch (JsonProcessingException e) {
                    System.out.println("Error converting into proper jsonNode" + addition.get(fieldName));
                    result.set(fieldName, addition.get(fieldName));
                }
            } else {
//                System.out.println("addition dont have fieldName: " + fieldName);
                result.set(fieldName, addition);
            }
        } else {
            System.out.println("addition is null for field:" + fieldName);
        }
        return result;
    }


    public static class JsonSerde extends Serdes.WrapperSerde<JsonNode> {
        public JsonSerde() {
            super(new JsonSerializer(), new JsonDeserializer());
        }
    }

    public static class JsonSerializer implements org.apache.kafka.common.serialization.Serializer<JsonNode> {
        public byte[] serialize(String topic, JsonNode data) {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class JsonDeserializer implements org.apache.kafka.common.serialization.Deserializer<JsonNode> {
        public JsonNode deserialize(String topic, byte[] data) {
            try {
                return mapper.readTree(data);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static JsonNode fetchPolicyConsent(String userId) throws IOException {
//        URL url = new URL("http://localhost:8080/policy/consent?userId=" + userId);
//        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
//        conn.setRequestMethod("GET");
//        conn.setConnectTimeout(3000);
//        conn.setReadTimeout(3000);
//        if (conn.getResponseCode() == 200) {
//            return mapper.readTree(conn.getInputStream());
//        }
//        return null;
        return mapper.createObjectNode().put("doNotSellMyDataConsent", "OPT_IN");
    }

}





