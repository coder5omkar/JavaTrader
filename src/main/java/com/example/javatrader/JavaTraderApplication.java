package com.example.javatrader;

import com.example.javatrader.model.TickerData;
import com.example.javatrader.repository.TickerDataRepository;
import com.example.javatrader.start.DhanWebSocketClient;
import com.example.javatrader.start.DhanWebSocketListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import jakarta.annotation.PostConstruct;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableKafka
public class JavaTraderApplication {

    private static final String ACCESS_TOKEN = "your_access_token";
    private static final String CLIENT_ID = "your_client_id";
    private static final String WS_URL = "wss://api-feed.dhan.co?version=2&token=" + ACCESS_TOKEN + "&clientId=" + CLIENT_ID + "&authType=2";

    private DhanWebSocketClient client;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @Autowired
    private TickerDataRepository tickerDataRepository;

    @Autowired
    private KafkaTemplate<String, TickerData> kafkaTemplate;

    public static void main(String[] args) {
        SpringApplication.run(JavaTraderApplication.class, args);
    }

    @PostConstruct
    public void start() {
        connectWithRetry();
    }

    private void connectWithRetry() {
        try {
            client = new DhanWebSocketClient(new URI(WS_URL), new DhanWebSocketListener() {
                @Override
                public void onConnected() {
                    System.out.println("WebSocket connected");
                }

                @Override
                public void onDisconnected() {
                    System.out.println("WebSocket disconnected, retrying...");
                    scheduleReconnect();
                }

                @Override
                public void onError(Exception ex) {
                    System.err.println("WebSocket error: " + ex.getMessage());
                    scheduleReconnect();
                }

                @Override
                public void onDataReceived(float ltp, int ltt) {
                    System.out.println("LTP: " + ltp + ", LTT: " + ltt);
                    saveTickerData(ltp, ltt);
                }
            });
            client.connect();
        } catch (Exception e) {
            System.err.println("Initial connection failed: " + e.getMessage());
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        scheduler.schedule(this::connectWithRetry, 10, TimeUnit.SECONDS);
    }

    private void saveTickerData(float ltp, int ltt) {
        LocalDateTime receivedAt = LocalDateTime.ofInstant(Instant.ofEpochSecond(ltt), ZoneId.systemDefault());
        TickerData data = new TickerData(ltp, ltt, receivedAt);

        // Publish to Kafka topic before saving to DB
        kafkaTemplate.send("ticker-data-topic", data);

        // Save to DB
        tickerDataRepository.save(data);
    }
}
