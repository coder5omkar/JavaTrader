package com.example.javatrader;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class DhanWebSocketClient extends WebSocketClient {

    private static volatile float lastTradedPrice;
    private static DhanWebSocketClient instance;
    private static final Set<Integer> subscribedOptionIds = new HashSet<>();
    private static final int STRIKE_RANGE = 50; // 50 points around current price for ATM
    private static String url ;

    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // or your Kafka server address
    private static final String NIFTY_TOPIC = "nifty-ltp";
    private static final String CALL_TOPIC = "call-ltp";
    private static final String PUT_TOPIC = "put-ltp";

    private final Producer<String, String> kafkaProducer;

    public DhanWebSocketClient(URI serverUri) {
        super(serverUri);
        instance = this;
        this.kafkaProducer = createKafkaProducer();
    }

    private Producer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return new KafkaProducer<>(props);
    }

    public static final Map<Integer, String> securityMap = new HashMap<>();
    static {
        securityMap.put(13, "NIFTY 50");
    }

    public static void main(String[] args) {
        try {
            String accessToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzQ4NjcyMzQ0LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMTIwODI3MyJ9.tP9wOg7vCVWnY0LPGsyq-tGmE7zCsFKHlcmnkdjHdN3eAG5xg1VkzgdU3nu7ce7dOXjX0H6cDn5hJZOEzyI3jw";
            String clientId = "1101208273";
            url = "wss://api-feed.dhan.co?version=2&token=" + accessToken + "&clientId=" + clientId + "&authType=2";
            DhanWebSocketClient client = new DhanWebSocketClient(new URI(url));
            client.connect();

            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(() -> {
                if (lastTradedPrice > 0) {
                    try {
                        System.out.println("option finder started.....");
                        instance.OptionFind(lastTradedPrice);
                        System.out.println("option finder completed.....");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, 0, 1, TimeUnit.MINUTES);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        System.out.println("Connected to DhanHQ WebSocket");

        String subscribeMessage = "{\n" +
                "  \"RequestCode\": 15,\n" +
                "  \"InstrumentCount\": 1,\n" +
                "  \"InstrumentList\": [\n" +
                "    {\n" +
                "      \"ExchangeSegment\": \"IDX_I\",\n" +
                "      \"SecurityId\": \"13\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        send(subscribeMessage);
    }

    public void subscribeATMOptions(List<Map<String, String>> instruments) {
        StringBuilder instrumentListBuilder = new StringBuilder();
        for (int i = 0; i < instruments.size(); i++) {
            Map<String, String> instrument = instruments.get(i);
            instrumentListBuilder.append("    {\n")
                    .append("      \"ExchangeSegment\": \"").append(instrument.get("ExchangeSegment")).append("\",\n")
                    .append("      \"SecurityId\": \"").append(instrument.get("SecurityId")).append("\"\n")
                    .append("    }");
            if (i < instruments.size() - 1) instrumentListBuilder.append(",\n");
            else instrumentListBuilder.append("\n");
        }

        String newSubscribeMessage = "{\n" +
                "  \"RequestCode\": 15,\n" +
                "  \"InstrumentCount\": " + instruments.size() + ",\n" +
                "  \"InstrumentList\": [\n" +
                instrumentListBuilder +
                "  ]\n" +
                "}";
        send(newSubscribeMessage);
    }

    public void unsubscribeOptions(Set<Integer> securityIds) {
        if (securityIds.isEmpty()) return;

        StringBuilder instrumentListBuilder = new StringBuilder();
        List<Integer> idList = new ArrayList<>(securityIds);
        for (int i = 0; i < idList.size(); i++) {
            instrumentListBuilder.append("    {\n")
                    .append("      \"ExchangeSegment\": \"NSE_FNO\",\n")
                    .append("      \"SecurityId\": \"").append(idList.get(i)).append("\"\n")
                    .append("    }");
            if (i < idList.size() - 1) instrumentListBuilder.append(",\n");
            else instrumentListBuilder.append("\n");
        }

        String unsubscribeMessage = "{\n" +
                "  \"RequestCode\": 16,\n" +
                "  \"InstrumentCount\": " + securityIds.size() + ",\n" +
                "  \"InstrumentList\": [\n" +
                instrumentListBuilder +
                "  ]\n" +
                "}";
        send(unsubscribeMessage);

        // Remove from tracking
        subscribedOptionIds.removeAll(securityIds);
        securityIds.forEach(securityMap::remove);

        System.out.println("Unsubscribed from options: " + securityIds);
    }

    @Override
    public void onMessage(String message) {
        System.out.println("Received message: " + message);
    }

    private final Map<Integer, Integer> tickCountMap = new ConcurrentHashMap<>();

    @Override
    public void onMessage(ByteBuffer bytes) {
        try {
            bytes.order(ByteOrder.LITTLE_ENDIAN);
            byte feedResponseCode = bytes.get();
            short messageLength = bytes.getShort();
            byte exchangeSegment = bytes.get();
            int securityId = bytes.getInt();
            float ltp = bytes.getFloat();
            int lastTradedTime = bytes.getInt();

            tickCountMap.putIfAbsent(securityId, 0);
            int tickCount = tickCountMap.get(securityId);

            if (tickCount < 3) {
                tickCountMap.put(securityId, tickCount + 1);
                return; // Skip initial ticks
            }

            String securityName = securityMap.getOrDefault(securityId, "UNKNOWN");
            LocalDateTime ltt = Instant.ofEpochSecond(lastTradedTime).atZone(ZoneId.systemDefault()).toLocalDateTime();

            lastTradedPrice = ltp;
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MMM yy hh:mm:ss a");
            String formattedDateTime = ltt.format(formatter);

            System.out.printf("%s (ID: %d) - LTP: ₹%.2f, LTT (Local): %s\n",
                    securityName, securityId, ltp, formattedDateTime);

            // Create JSON message for Kafka
            String jsonMessage = String.format("{\"securityId\":%d,\"securityName\":\"%s\",\"ltp\":%.2f,\"timestamp\":\"%s\"}",
                    securityId, securityName, ltp, formattedDateTime);

            // Publish to appropriate Kafka topic
            if (securityName.equals("NIFTY 50")) {
                kafkaProducer.send(new ProducerRecord<>(NIFTY_TOPIC, String.valueOf(securityId), jsonMessage));
                DatabaseUtil.saveToDB("nifty_data", securityId, securityName, ltp, ltt);
            } else if (securityName.endsWith("CALL")) {
                kafkaProducer.send(new ProducerRecord<>(CALL_TOPIC, String.valueOf(securityId), jsonMessage));
                DatabaseUtil.saveToDB("call_data", securityId, securityName, ltp, ltt);
            } else if (securityName.endsWith("PUT")) {
                kafkaProducer.send(new ProducerRecord<>(PUT_TOPIC, String.valueOf(securityId), jsonMessage));
                DatabaseUtil.saveToDB("put_data", securityId, securityName, ltp, ltt);
            }

        } catch (Exception e) {
            System.err.println("Error parsing binary message: " + e.getMessage());
        }
    }


    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("WebSocket closed. Code: " + code + ", Reason: " + reason);
        // Close Kafka producer
        kafkaProducer.close();

        // Optional: add delay before reconnect
        try {
            Thread.sleep(5000); // 5 seconds delay
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Reconnect logic
        reconnectWebSocket();
    }

    public void reconnectWebSocket() {
        try {
            // Ensure you properly clean up or reuse your existing WebSocket client
            DhanWebSocketClient newClient = new DhanWebSocketClient(new URI(url));
            newClient.connect();
        } catch (Exception e) {
            System.err.println("WebSocket reconnection failed: " + e.getMessage());
        }
    }

    @Override
    public void onError(Exception ex) {
        System.err.println("WebSocket error: " + ex.getMessage());
    }

    public void OptionFind(float price) throws IOException {
        String filePath = "instrument_cache.csv";
        int spotPrice = Math.round(price);
        System.out.println("price nifty to be searching -" +price);
        String index = "NIFTY";

        OptionFinder.OptionData atmCall = findATMOption(filePath, spotPrice, "CALL", index);
        OptionFinder.OptionData atmPut = findATMOption(filePath, spotPrice, "PUT", index);

        System.out.println("ATM CALL: " + (atmCall != null ? atmCall.customSymbol + " - Security ID: " + atmCall.securityId : "Not Found"));
        System.out.println("ATM PUT:  " + (atmPut != null ? atmPut.customSymbol + " - Security ID: " + atmPut.securityId : "Not Found"));

        // Check if we already have subscribed options
        Set<Integer> existingOptionIds = securityMap.keySet().stream()
                .filter(id -> id != 13) // Exclude NIFTY index
                .collect(Collectors.toSet());

        boolean needToUpdate = false;

        if (atmCall != null && !existingOptionIds.contains(atmCall.securityId)) {
            needToUpdate = true;
        }
        if (atmPut != null && !existingOptionIds.contains(atmPut.securityId)) {
            needToUpdate = true;
        }

        if (!needToUpdate) {
            System.out.println("Same ATM CALL and PUT already subscribed. Skipping option finder.");
            return;
        }

        // Unsubscribe existing options if needed
        if (!existingOptionIds.isEmpty()) {
            unsubscribeOptions(existingOptionIds);
        }

        // Subscribe to new options
        if (atmCall != null) {
            securityMap.put(atmCall.securityId, atmCall.customSymbol);
            subscribedOptionIds.add(atmCall.securityId);
        }
        if (atmPut != null) {
            securityMap.put(atmPut.securityId, atmPut.customSymbol);
            subscribedOptionIds.add(atmPut.securityId);
        }

        List<Map<String, String>> instruments = new ArrayList<>();
        if (atmCall != null) {
            Map<String, String> callMap = new HashMap<>();
            callMap.put("ExchangeSegment", "NSE_FNO");
            callMap.put("SecurityId", String.valueOf(atmCall.securityId));
            instruments.add(callMap);
        }
        if (atmPut != null) {
            Map<String, String> putMap = new HashMap<>();
            putMap.put("ExchangeSegment", "NSE_FNO");
            putMap.put("SecurityId", String.valueOf(atmPut.securityId));
            instruments.add(putMap);
        }

        if (!instruments.isEmpty()) {
            subscribeATMOptions(instruments);
        }
    }

    public static OptionFinder.OptionData findATMOption(String filePath, int spotPrice, String type, String index) throws IOException {
        LocalDate today = LocalDate.now();

        return Files.lines(Paths.get(filePath))
                .skip(1)
                .map(line -> line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
                .filter(tokens -> tokens.length > 7)
                .map(tokens -> {
                    try {
                        String symbol = tokens[7].replaceAll("\"", "").trim();
                        if (!symbol.startsWith(index + " ") || !symbol.endsWith(type)) return null;

                        int strike = extractStrike(symbol);
                        LocalDate expiry = extractExpiryDate(symbol);
                        if (expiry == null || expiry.isBefore(today) || expiry.getDayOfWeek() != DayOfWeek.THURSDAY) return null;

                        // Check if strike is within range
                        if (Math.abs(strike - spotPrice) > STRIKE_RANGE) return null;

                        int secId = Integer.parseInt(tokens[2].trim());
//                        System.out.println("Expiry.." + expiry);

                        OptionFinder.OptionData data = new OptionFinder.OptionData();
                        data.customSymbol = symbol;
                        data.strikePrice = strike;
                        data.expiryDate = expiry;
                        data.type = type;
                        data.securityId = secId;
                        return data;
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .min(Comparator
                        .comparingInt((OptionFinder.OptionData o) -> Math.abs(o.strikePrice - spotPrice))
                        .thenComparing(o -> o.expiryDate))
                .orElse(null);
    }

    private static int extractStrike(String symbol) {
        String[] parts = symbol.split(" ");
        for (int i = parts.length - 1; i >= 0; i--) {
            if (parts[i].matches("\\d+")) {
                return Integer.parseInt(parts[i]);
            }
        }
        return -1;
    }

    private static LocalDate extractExpiryDate(String symbol) {
        try {
            String[] parts = symbol.split(" ");
            int day = Integer.parseInt(parts[1]);
            String monthStr = parts[2].toUpperCase();
            Month month;

            switch (monthStr) {
                case "JAN": month = Month.JANUARY; break;
                case "FEB": month = Month.FEBRUARY; break;
                case "MAR": month = Month.MARCH; break;
                case "APR": month = Month.APRIL; break;
                case "MAY": month = Month.MAY; break;
                case "JUN": month = Month.JUNE; break;
                case "JUL": month = Month.JULY; break;
                case "AUG": month = Month.AUGUST; break;
                case "SEP": month = Month.SEPTEMBER; break;
                case "OCT": month = Month.OCTOBER; break;
                case "NOV": month = Month.NOVEMBER; break;
                case "DEC": month = Month.DECEMBER; break;
                default: return null;
            }

            int year = Year.now().getValue();
            LocalDate date = LocalDate.of(year, month, day);


            if (date.isBefore(LocalDate.now())) {
                date = date.plusYears(1);
            }
//            System.out.println("Symbol: " + symbol + ", parts: " + Arrays.toString(parts) +"date: " + date);

            return date;
        } catch (Exception e) {
            return null;
        }
    }

}