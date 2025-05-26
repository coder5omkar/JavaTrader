package com.example.javatrader;

import com.example.javatrader.model.TickerData;
import com.example.javatrader.repository.TickerDataRepository;
import com.example.javatrader.start.DhanWebSocketClient;
import com.example.javatrader.start.DhanWebSocketListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@SpringBootApplication
@EnableKafka
public class JavaTraderApplication {
    private static final Logger logger = LoggerFactory.getLogger(JavaTraderApplication.class);

    @Value("${dhan.access.token}")
    private String accessToken;

    @Value("${dhan.client.id}")
    private String clientId;

    @Value("${dhan.ws.url}")
    private String wsUrlTemplate;

    @Value("${instrument.list.refresh.hours:24}")
    private int instrumentListRefreshHours;

    @Value("${nifty.options.strike.multiple:50}")
    private int strikeMultiple;

    @Value("${nifty.symbol:NIFTY}")
    private String niftySymbol;

    @Value("${nifty.security.id:13}")
    private String niftySecurityId;

    @Autowired
    private TickerDataRepository tickerDataRepository;

    @Autowired
    private KafkaTemplate<String, TickerData> kafkaTemplate;

    private DhanWebSocketClient client;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean subscribedToOptions = new AtomicBoolean(false);
    private volatile Map<String, Map<String, String>> instrumentLookup = new ConcurrentHashMap<>();
    private ScheduledExecutorService instrumentListRefresher;
    private volatile float lastSubscribedNiftyPrice = 0;
    private String currentCallId;
    private String currentPutId;

    public static void main(String[] args) {
        SpringApplication.run(JavaTraderApplication.class, args);
    }

    @PostConstruct
    public void start() {
        initInstrumentList();
        scheduleInstrumentListRefresh();
        connectWithRetry();
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down application");
        scheduler.shutdown();
        if (instrumentListRefresher != null) {
            instrumentListRefresher.shutdown();
        }
        if (client != null) {
            client.close();
        }
    }

    private void scheduleInstrumentListRefresh() {
        instrumentListRefresher = Executors.newSingleThreadScheduledExecutor();
        instrumentListRefresher.scheduleAtFixedRate(
                this::initInstrumentList,
                instrumentListRefreshHours,
                instrumentListRefreshHours,
                TimeUnit.HOURS
        );
    }

    private void initInstrumentList() {
        try {
            Path cachedFile = Paths.get("instrument_cache.csv");

            // Try to download fresh copy
            try {
                URL url = new URL("https://images.dhan.co/api-data/api-scrip-master.csv");
                Files.copy(url.openStream(), cachedFile, StandardCopyOption.REPLACE_EXISTING);
                logger.info("Successfully downloaded fresh instrument list");
            } catch (IOException e) {
                logger.warn("Failed to download instrument list, using cached version", e);
                if (!Files.exists(cachedFile)) {
                    throw new IllegalStateException("No instrument list available");
                }
            }

            // Process the file (either fresh or cached)
            try (BufferedReader reader = Files.newBufferedReader(cachedFile)) {
                Map<String, Map<String, String>> newLookup = new ConcurrentHashMap<>();

                reader.lines()
                        .skip(1)
                        .parallel()
                        .map(line -> line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
                        .filter(tokens -> tokens.length > 12)
                        .forEach(tokens -> {
                            String key = buildInstrumentKey(
                                    tokens[0],  // ExchangeSegment
                                    tokens[4],  // Underlying
                                    tokens[5],  // StrikePrice
                                    tokens[6],  // OptionType
                                    tokens[7]   // ExpiryDate
                            );

                            Map<String, String> instrument = new HashMap<>();
                            instrument.put("ExchangeSegment", tokens[0]);
                            instrument.put("InstrumentType", tokens[3]);
                            instrument.put("Underlying", tokens[4]);
                            instrument.put("StrikePrice", tokens[5]);
                            instrument.put("OptionType", tokens[6]);
                            instrument.put("ExpiryDate", tokens[7]);
                            instrument.put("SecurityId", tokens[1]);
                            instrument.put("CustomSymbol", tokens[8]);

                            newLookup.put(key, instrument);
                        });

                this.instrumentLookup = newLookup;
                logger.info("Loaded instrument lookup with {} entries", newLookup.size());
            }
        } catch (Exception e) {
            logger.error("Error loading instrument list", e);
        }
    }

    private String buildInstrumentKey(String exchange, String underlying, String strike,
                                      String optionType, String expiry) {
        return String.format("%s|%s|%s|%s|%s",
                exchange.toUpperCase(),
                underlying.toUpperCase(),
                strike,
                optionType.toUpperCase(),
                expiry.toUpperCase());
    }

    private void connectWithRetry() {
        try {
            String wsUrl = String.format(wsUrlTemplate, accessToken, clientId);
            client = new DhanWebSocketClient(new URI(wsUrl), new DhanWebSocketListener() {
                @Override
                public void onConnected() {
                    logger.info("WebSocket connected");
                    subscribedToOptions.set(false);
                    // Subscribe to NIFTY index immediately after connection
                    subscribeToNiftyIndex();
                }

                @Override
                public void onDisconnected() {
                    logger.warn("WebSocket disconnected");
                    scheduleReconnect();
                }

                @Override
                public void onError(Exception ex) {
                    logger.error("WebSocket error", ex);
                    scheduleReconnect();
                }

                @Override
                public void onDataReceived(float ltp, int ltt) {
                    logger.debug("Market data - LTP: {}, LTT: {}", ltp, ltt);
                    saveTickerData(ltp, ltt);

                    // Check if we need to subscribe to new options
                    if (Math.abs(ltp - lastSubscribedNiftyPrice) >= strikeMultiple || !subscribedToOptions.get()) {
                        subscribeToAtmOptions(ltp);
                    }
                }

                @Override
                public void onTextMessage(String message) {
                    logger.debug("Received text message: {}", message);
                }
            });
            client.connect();
        } catch (Exception e) {
            logger.error("Initial connection failed", e);
            scheduleReconnect();
        }
    }

    private void subscribeToNiftyIndex() {
        String subscriptionMessage = String.format(
                "{\"action\":\"subscribe\",\"instruments\":[{" +
                        "\"exchangeSegment\":\"NSE_INDICES\",\"instrumentId\":\"%s\"}]}",
                niftySecurityId
        );
        client.sendWithGuarantee(subscriptionMessage);
        logger.info("Subscribed to NIFTY index");
    }

    private void subscribeToAtmOptions(float currentPrice) {
        int strike = Math.round(currentPrice / strikeMultiple) * strikeMultiple;
        List<String> expiryDates = getValidExpiryDates();

        for (String expiry : expiryDates) {
            String callId = findInstrumentId(niftySymbol, strike, "CE", expiry);
            String putId = findInstrumentId(niftySymbol, strike, "PE", expiry);

            if (callId != null && putId != null) {
                // First unsubscribe from previous options if any
                if (currentCallId != null && currentPutId != null) {
                    unsubscribeFromOptions(currentCallId, currentPutId);
                }

                // Subscribe to new options
                subscribeToOptionInstruments(callId, putId);

                // Update tracking variables
                lastSubscribedNiftyPrice = currentPrice;
                currentCallId = callId;
                currentPutId = putId;
                subscribedToOptions.set(true);

                logger.info("Subscribed to ATM options: Price={}, Strike={}, Expiry={}, Call={}, Put={}",
                        currentPrice, strike, expiry, callId, putId);
                break;
            }
        }

        if (!subscribedToOptions.get()) {
            logger.warn("Failed to find option instruments for current price: {}", currentPrice);
        }
    }

    private void unsubscribeFromOptions(String callId, String putId) {
        try {
            // Construct the unsubscribe message as per DhanHQ's API v2
            Map<String, Object> unsubscribeMessage = new HashMap<>();
            unsubscribeMessage.put("RequestCode", 16); // 16 indicates an unsubscribe request
            unsubscribeMessage.put("InstrumentCount", 2);

            List<Map<String, String>> instrumentList = new ArrayList<>();

            Map<String, String> callInstrument = new HashMap<>();
            callInstrument.put("ExchangeSegment", "NSE_FNO");
            callInstrument.put("SecurityId", callId);

            Map<String, String> putInstrument = new HashMap<>();
            putInstrument.put("ExchangeSegment", "NSE_FNO");
            putInstrument.put("SecurityId", putId);

            instrumentList.add(callInstrument);
            instrumentList.add(putInstrument);

            unsubscribeMessage.put("InstrumentList", instrumentList);

            // Convert the message to JSON
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonMessage = objectMapper.writeValueAsString(unsubscribeMessage);

            // Send the unsubscribe message over the WebSocket
            client.sendWithGuarantee(jsonMessage);
            logger.info("Unsubscribed from previous options: Call={}, Put={}", callId, putId);
        } catch (Exception e) {
            logger.error("Failed to unsubscribe from options", e);
        }
    }

    private List<String> getValidExpiryDates() {
        LocalDate today = LocalDate.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy");

        Set<String> uniqueExpiries = instrumentLookup.values().stream()
                .map(instr -> (String) instr.get("ExpiryDate"))  // Cast to String
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        return uniqueExpiries.stream()
                .filter(expiry -> {
                    try {
                        LocalDate expiryDate = LocalDate.parse(expiry, formatter);
                        return !expiryDate.isBefore(today) &&
                                expiryDate.getDayOfWeek() == DayOfWeek.THURSDAY;
                    } catch (Exception e) {
                        return false;
                    }
                })
                .sorted(Comparator.comparing(expiry -> {
                    try {
                        return LocalDate.parse(expiry, formatter);
                    } catch (Exception e) {
                        return LocalDate.MAX;
                    }
                }))
                .collect(Collectors.toList());
    }

    private void subscribeToOptionInstruments(String callId, String putId) {
        try {
            Map<String, Object> subscriptionMessage = new HashMap<>();
            subscriptionMessage.put("RequestCode", 15); // 15 = subscribe request
            subscriptionMessage.put("InstrumentCount", 2);

            List<Map<String, String>> instrumentList = new ArrayList<>();

            Map<String, String> callInstrument = new HashMap<>();
            callInstrument.put("ExchangeSegment", "NSE_FNO");
            callInstrument.put("SecurityId", callId);

            Map<String, String> putInstrument = new HashMap<>();
            putInstrument.put("ExchangeSegment", "NSE_FNO");
            putInstrument.put("SecurityId", putId);

            instrumentList.add(callInstrument);
            instrumentList.add(putInstrument);

            subscriptionMessage.put("InstrumentList", instrumentList);

            ObjectMapper objectMapper = new ObjectMapper();
            String jsonMessage = objectMapper.writeValueAsString(subscriptionMessage);

            client.sendWithGuarantee(jsonMessage);
            logger.info("Subscribed to options: Call={}, Put={}", callId, putId);
        } catch (Exception e) {
            logger.error("Subscription failed", e);
        }
    }

    private void scheduleReconnect() {
        scheduler.schedule(this::connectWithRetry, 10, TimeUnit.SECONDS);
    }

    private void saveTickerData(float ltp, int ltt) {
        try {
            LocalDateTime receivedAt = LocalDateTime.ofInstant(Instant.ofEpochSecond(ltt), ZoneId.systemDefault());
            TickerData data = new TickerData(ltp, ltt, receivedAt);

            scheduler.execute(() -> {
                kafkaTemplate.send("ticker-data-topic", data)
                        .thenAccept(result -> logger.debug("Sent tick data to Kafka"))
                        .exceptionally(ex -> {
                            logger.error("Failed to send tick data to Kafka", ex);
                            return null;
                        });

                tickerDataRepository.save(data);
            });
        } catch (Exception e) {
            logger.error("Error saving tick data", e);
        }
    }

    private String findInstrumentId(String underlying, int strikePrice, String optionType, String expiryDate) {
        String key = buildInstrumentKey(
                "NSE",
                underlying,
                String.valueOf(strikePrice),
                optionType,
                expiryDate
        );

        Map<String, String> instrument = instrumentLookup.get(key);
        if (instrument != null) {
            return instrument.get("SecurityId");
        }

        String searchPattern = String.format("%s %s %d %s",
                underlying,
                expiryDate.split("-")[1],
                strikePrice,
                optionType.equals("CE") ? "CALL" : "PUT");

        Optional<Map.Entry<String, Map<String, String>>> match = instrumentLookup.entrySet().parallelStream()
                .filter(e -> {
                    String customSymbol = e.getValue().get("CustomSymbol");
                    return customSymbol != null && customSymbol.contains(searchPattern);
                })
                .findFirst();

        return match.map(e -> e.getValue().get("SecurityId")).orElse(null);
    }
}