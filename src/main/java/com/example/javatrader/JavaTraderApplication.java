package com.example.javatrader;

import com.example.javatrader.model.TickerData;
import com.example.javatrader.repository.TickerDataRepository;
import com.example.javatrader.start.DhanWebSocketClient;
import com.example.javatrader.start.DhanWebSocketListener;
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
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
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

    private DhanWebSocketClient client;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean subscribedToOptions = new AtomicBoolean(false);
    private volatile Map<String, Map<String, String>> instrumentLookup = new ConcurrentHashMap<>();
    private ScheduledExecutorService instrumentListRefresher;

    @Autowired
    private TickerDataRepository tickerDataRepository;

    @Autowired
    private KafkaTemplate<String, TickerData> kafkaTemplate;

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

                    if (!subscribedToOptions.get()) {
                        int strike = Math.round(ltp / strikeMultiple) * strikeMultiple;
                        String expiry = getNearestThursdayExpiry();

                        String callId = findInstrumentId(niftySymbol, strike, "CE", expiry);
                        String putId = findInstrumentId(niftySymbol, strike, "PE", expiry);

                        if (callId != null && putId != null) {
                            subscribeToOptionInstruments(callId, putId);
                            subscribedToOptions.set(true);
                            logger.info("Subscribed to ATM options: Strike={}, Call={}, Put={}",
                                    strike, callId, putId);
                        } else {
                            logger.warn("Failed to find option instruments for strike: {}", strike);
                        }
                    }
                }
            });
            client.connect();
        } catch (Exception e) {
            logger.error("Initial connection failed", e);
            scheduleReconnect();
        }
    }

    private void subscribeToOptionInstruments(String callId, String putId) {
        String subscriptionMessage = String.format(
                "{\"action\":\"subscribe\",\"instruments\":[{" +
                        "\"exchangeSegment\":\"NSE_FNO\",\"instrumentId\":\"%s\"}," +
                        "{\"exchangeSegment\":\"NSE_FNO\",\"instrumentId\":\"%s\"}]}",
                callId, putId
        );
        client.sendWithGuarantee(subscriptionMessage);
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
        return instrument != null ? instrument.get("SecurityId") : null;
    }

    private String getNearestThursdayExpiry() {
        LocalDate today = LocalDate.now();
        LocalDate nextThursday = today.with(TemporalAdjusters.nextOrSame(DayOfWeek.THURSDAY));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy");
        return nextThursday.format(formatter).toUpperCase();
    }
}