package com.example.javatrader;

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

public class DhanWebSocketClient extends WebSocketClient {

    private static volatile float lastTradedPrice;
    private static DhanWebSocketClient instance;

    public DhanWebSocketClient(URI serverUri) {
        super(serverUri);
        instance = this;
    }

    public static final Map<Integer, String> securityMap = new HashMap<>();
    static {
        securityMap.put(13, "NIFTY 50");
    }

    public static void main(String[] args) {
        try {
            String accessToken = "YOUR_ACCESS_TOKEN";
            String clientId = "YOUR_CLIENT_ID";
            String url = "wss://api-feed.dhan.co?version=2&token=" + accessToken + "&clientId=" + clientId + "&authType=2";
            DhanWebSocketClient client = new DhanWebSocketClient(new URI(url));
            client.connect();

            // Schedule OptionFind() every 40 minutes
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(() -> {
                if (lastTradedPrice > 0) {
                    try {
                        instance.OptionFind(lastTradedPrice);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, 0, 40, TimeUnit.MINUTES);

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

    @Override
    public void onMessage(String message) {
        System.out.println("Received message: " + message);
    }

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

            String securityName = securityMap.getOrDefault(securityId, "UNKNOWN");
            LocalDateTime ltt = Instant.ofEpochSecond(lastTradedTime).atZone(ZoneId.systemDefault()).toLocalDateTime();

            lastTradedPrice = ltp;

            LocalDateTime now = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MMM yy hh:mm:ss a");
            String formattedDateTime = now.format(formatter);

            System.out.printf("%s (ID: %d) - LTP: ₹%.2f, LTT (Local): %s\n",
                    securityName, securityId, ltp, formattedDateTime);
            System.out.println("_____________________________________________________________");

            if (securityName.equals("NIFTY 50")) {
                DatabaseUtil.saveToDB("nifty_data", securityId, securityName, ltp, ltt);
            } else if (securityName.endsWith("CALL")) {
                DatabaseUtil.saveToDB("call_data", securityId, securityName, ltp, ltt);
            } else if (securityName.endsWith("PUT")) {
                DatabaseUtil.saveToDB("put_data", securityId, securityName, ltp, ltt);
            }

        } catch (Exception e) {
            System.err.println("Error parsing binary message: " + e.getMessage());
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("WebSocket closed. Code: " + code + ", Reason: " + reason);
    }

    @Override
    public void onError(Exception ex) {
        System.err.println("WebSocket error: " + ex.getMessage());
    }

    public void OptionFind(float price) throws IOException {
        String filePath = "instrument_cache.csv";
        int spotPrice = Math.round(price);
        String index = "NIFTY";

        OptionFinder.OptionData atmCall = findATMOption(filePath, spotPrice, "CALL", index);
        OptionFinder.OptionData atmPut = findATMOption(filePath, spotPrice, "PUT", index);

        System.out.println("ATM CALL: " + (atmCall != null ? atmCall.customSymbol + " - Security ID: " + atmCall.securityId : "Not Found"));
        System.out.println("ATM PUT:  " + (atmPut != null ? atmPut.customSymbol + " - Security ID: " + atmPut.securityId : "Not Found"));

        if (atmCall != null) securityMap.put(atmCall.securityId, atmCall.customSymbol);
        if (atmPut != null) securityMap.put(atmPut.securityId, atmPut.customSymbol);

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

                        int secId = Integer.parseInt(tokens[2].trim());

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
            Month month = Month.valueOf(parts[2].toUpperCase());
            int year = Year.now().getValue();
            LocalDate date = LocalDate.of(year, month, day);
            if (date.isBefore(LocalDate.now())) date = date.plusYears(1);
            return date;
        } catch (Exception e) {
            return null;
        }
    }
}
