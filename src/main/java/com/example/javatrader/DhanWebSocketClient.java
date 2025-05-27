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

public class DhanWebSocketClient extends WebSocketClient {

    public DhanWebSocketClient(URI serverUri) {
        super(serverUri);
    }

    public static void main(String[] args) {
        try {
            String accessToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzQ4NDE2NTIwLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMTIwODI3MyJ9.aKANXnP3hkgtPLEx8yL9dYOruZxpRaRt2q3blu1_bvbCFQ-apcvrU9wKfP1UQXLl1Ry07Ui_PapdjZtOoPabhA";
            String clientId = "1101208273";

            String url = "wss://api-feed.dhan.co?version=2&token=" + accessToken + "&clientId=" + clientId + "&authType=2";
            DhanWebSocketClient client = new DhanWebSocketClient(new URI(url));
            client.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static final Map<Integer, String> securityMap = new HashMap<>();

    static {
        securityMap.put(13, "NIFTY 50");
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        System.out.println("Connected to DhanHQ WebSocket");

        // Subscribe to multiple instruments: NIFTY 50 and two NSE_FNO securities with ID 61727
        String subscribeMessage = "{\n" +
                "  \"RequestCode\": 15,\n" +
                "  \"InstrumentCount\": 1,\n" +
                "  \"InstrumentList\": [\n" +
                "    {\n" +
                "      \"ExchangeSegment\": \"IDX_I\",\n" +
                "      \"SecurityId\": \"13\"\n" +
                "    },\n" +
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

            if (i < instruments.size() - 1) {
                instrumentListBuilder.append(",\n");
            } else {
                instrumentListBuilder.append("\n");
            }
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
            bytes.order(ByteOrder.LITTLE_ENDIAN); // DHAN uses little-endian

            // Read header (8 bytes)
            byte feedResponseCode = bytes.get(); // 1 byte
            short messageLength = bytes.getShort(); // 2 bytes
            byte exchangeSegment = bytes.get(); // 1 byte
            int securityId = bytes.getInt(); // 4 bytes

            String securityName = securityMap.getOrDefault(securityId, "UNKNOWN");

            // Read payload
            float lastTradedPrice = bytes.getFloat(); // 4 bytes
            int lastTradedTime = bytes.getInt(); // e.g., 1716805200
            LocalDateTime ltt = Instant.ofEpochSecond(lastTradedTime)
                    .atZone(ZoneId.systemDefault())
                    .toLocalDateTime();      // 4 bytes

            // Convert LTP (already in rupees)
            double ltp = lastTradedPrice;

            // Convert LTT (treat as IST timezone)
//            ZoneId istZone = ZoneId.of("Asia/Kolkata");
//            LocalDateTime istTime = LocalDateTime.ofInstant(
//                    Instant.ofEpochSecond(lastTradedTime),
//                    istZone
//            );

            // Convert to your local timezone (optional)
            LocalDateTime now = LocalDateTime.now(); // Get current local date and time
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MMM yy hh:mm:ss a"); // Format: 27 May 25 04:21 PM
            String formattedDateTime = now.format(formatter);
            System.out.println(formattedDateTime);

            System.out.printf("%s (ID: %d) - LTP: ₹%.2f, LTT (Local):",
                    securityName,
                    securityId,
                    lastTradedPrice,
                    formattedDateTime);

            System.out.println("_____________________________________________________________");
            // Save data
            if (securityName.equals("NIFTY 50")) {
                DatabaseUtil.saveToDB("nifty_data", securityId, securityName, ltp, ltt);
            } else if (securityName.endsWith("CALL")) {
                DatabaseUtil.saveToDB("call_data", securityId, securityName, ltp, ltt);
            } else if (securityName.endsWith("PUT")) {
                DatabaseUtil.saveToDB("put_data", securityId, securityName, ltp, ltt);
            }

            //option find method call
            OptionFind(lastTradedPrice);

        } catch (Exception e) {
            System.err.println("Error parsing binary message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("WebSocket closed. Code: " + code + ", Reason: " + reason + ", Remote: " + remote);
    }

    @Override
    public void onError(Exception ex) {
        System.err.println("WebSocket error: " + ex.getMessage());
    }

    static class OptionData {
        String customSymbol;
        int strikePrice;
        LocalDate expiryDate;
        String type;
        int securityId;
    }

    public void OptionFind(float price) throws IOException {
        String filePath = "instrument_cache.csv";
        int spotPrice = Integer.parseInt(String.valueOf(price));
        String index = "NIFTY";

        OptionFinder.OptionData atmCall = findATMOption(filePath, spotPrice, "CALL", index);
        OptionFinder.OptionData atmPut = findATMOption(filePath, spotPrice, "PUT", index);

        System.out.println("ATM CALL: " + (atmCall != null ? atmCall.customSymbol + " - Security ID: " + atmCall.securityId : "Not Found"));
        System.out.println("ATM PUT:  " + (atmPut != null ? atmPut.customSymbol + " - Security ID: " + atmPut.securityId : "Not Found"));

        // Add to global security map
        if (atmCall != null) {
            securityMap.put(atmCall.securityId, atmCall.customSymbol);
        }
        if (atmPut != null) {
            securityMap.put(atmPut.securityId, atmPut.customSymbol);
        }

        Map<String, String> map = new HashMap();
        map.put(atmCall.customSymbol, String.valueOf(atmCall.securityId));
        map.put(atmPut.customSymbol, String.valueOf(atmPut.securityId));

        List<Map<String, String>> instruments = new ArrayList<>();
        instruments.add(map);

        subscribeATMOptions(instruments);
    }

    public static OptionFinder.OptionData findATMOption(String filePath, int spotPrice, String type, String index) throws IOException {
        LocalDate today = LocalDate.now();

        return Files.lines(Paths.get(filePath))
                .skip(1)
                .map(line -> line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
                .filter(tokens -> tokens.length > 7)
                .map(tokens -> {
                    try {
                        String symbol = tokens[7].replaceAll("\"", "").trim(); // SEM_CUSTOM_SYMBOL
                        if (!symbol.startsWith(index + " ")) return null;
                        if (!symbol.endsWith(type)) return null;

                        int strike = extractStrike(symbol);
                        LocalDate expiry = extractExpiryDate(symbol);
                        if (expiry == null || expiry.isBefore(today) || expiry.getDayOfWeek() != DayOfWeek.THURSDAY) {
                            return null;
                        }

                        int secId = Integer.parseInt(tokens[2].trim()); // SEM_SMST_SECURITY_I

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
            // Example: NIFTY 29 MAY 18250 CALL → extract 29 MAY
            String[] parts = symbol.split(" ");
            int day = Integer.parseInt(parts[1]);
            Month month = Month.valueOf(parts[2].toUpperCase());
            int year = Year.now().getValue(); // Assume current year

            LocalDate date = LocalDate.of(year, month, day);
            // If this date has already passed this year, assume next year expiry
            if (date.isBefore(LocalDate.now())) {
                date = date.plusYears(1);
            }

            return date;
        } catch (Exception e) {
            return null;
        }
    }
}