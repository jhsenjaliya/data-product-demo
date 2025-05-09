package org.jaysen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GeoJsonLookup {
    private static final Map<String, String> addressToPostcodeMap = new HashMap<>();

    static {
        // Load GeoJSON into memory once at startup
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = null;
        try {
            root = mapper.readTree(new File("openaddresses/source.geojson"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (JsonNode feature : root.get("features")) {
            JsonNode props = feature.get("properties");
            String key = props.get("number").asText() + " " +
                    props.get("street").asText() + "," +
                    props.get("city").asText() + "," +
                    props.get("region").asText();
            addressToPostcodeMap.put(key.toLowerCase(), props.get("postcode").asText());
        }
    }

    public static String getPostcode(String street, String city, String region) {
        return addressToPostcodeMap.getOrDefault(
                street.toLowerCase() + "," + city.toLowerCase() + "," + region.toLowerCase(), "UNKNOWN"
        );
    }
}
