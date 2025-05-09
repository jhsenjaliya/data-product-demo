package org.jaysen;

public class BusinessEventBuilder {
    public static void main(String[] args) {
        // Simplified illustration of enrichment and snapshot logic

        builder.stream("user-domain-cdc-events")
                .foreach((key, cdcEvent) -> {
                    String userId = cdcEvent.getPayload().get("user_id").asText();

// Query latest full user data from ScyllaDB
                    try (CqlSession session = CqlSession.builder().build()) {
                        Row userRow = session.execute(
                                SimpleStatement.newInstance("SELECT * FROM user_domain WHERE user_id = ?", UUID.fromString(userId))
                        ).one();

                        List<Row> addressRows = session.execute(
                                SimpleStatement.newInstance("SELECT * FROM user_addresses WHERE user_id = ?", UUID.fromString(userId))
                        ).all();

                        ObjectMapper mapper = new ObjectMapper();
                        ObjectNode fullEvent = mapper.createObjectNode();
                        fullEvent.put("user_id", userId);

// Build user profile
                        ObjectNode profile = mapper.createObjectNode();
                        profile.put("name", userRow.getString("profile_name"));
                        profile.put("email", userRow.getString("profile_email"));
                        fullEvent.set("profile", profile);

// Preferences
                        ObjectNode preferences = mapper.createObjectNode();
                        preferences.put("language", userRow.getString("preferences_language"));
                        preferences.put("timezone", userRow.getString("preferences_timezone"));
                        fullEvent.set("preferences", preferences);

// Enrich addresses with postcode from GeoJSON lookup
                        ArrayNode addressesArray = mapper.createArrayNode();
                        for (Row addr : addressRows) {
                            ObjectNode addrNode = mapper.createObjectNode();
                            addrNode.put("street", addr.getString("street"));
                            addrNode.put("city", addr.getString("city"));
                            addrNode.put("region", addr.getString("region"));

// GeoJSON-based postcode enrichment
                            String postcode = GeoJsonLookup.getPostcode(
                                    addr.getString("street"), addr.getString("city"), addr.getString("region")
                            );
                            addrNode.put("postcode", postcode);

                            addressesArray.add(addrNode);
                        }
                        fullEvent.set("addresses", addressesArray);

// Producer-side metric
                        fullEvent.put("profile_completion_score", (profile.size() + preferences.size()) * 20);
                        fullEvent.put("event_timestamp", Instant.now().toString());

                        // Publish to Kafka full event topic
                        kafkaProducer.send(new ProducerRecord<>("user-domain-full-events", userId, fullEvent.toString()));

                        // Write snapshot to ScyllaDB warehouse table
                        session.execute(
                                SimpleStatement.newInstance(
                                        "INSERT INTO user_domain_snapshot (user_id, snapshot_json, event_time) VALUES (?, ?, ?)",
                                        UUID.fromString(userId),
                                        fullEvent.toString(),
                                        Instant.now()
                                )
                        );

                    } catch (Exception e) {
                        // Exception handling
                    }
                });

    }
}




