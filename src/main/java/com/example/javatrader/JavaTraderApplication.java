package com.example.javatrader;

import com.example.javatrader.start.DhanWebSocketClient;
import com.example.javatrader.start.DhanWebSocketListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class JavaTraderApplication {

    private static final String ACCESS_TOKEN = "your_access_token";
    private static final String CLIENT_ID = "your_client_id";
    private static final String WS_URL = "wss://api-feed.dhan.co?version=2&token=" + ACCESS_TOKEN + "&clientId=" + CLIENT_ID + "&authType=2";

    private static DhanWebSocketClient client;
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public static void main(String[] args) {
        SpringApplication.run(JavaTraderApplication.class, args);
        connectWithRetry();
    }

    private static void connectWithRetry() {
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
                }
            });
            client.connect();
        } catch (Exception e) {
            System.err.println("Initial connection failed: " + e.getMessage());
            scheduleReconnect();
        }
    }

    private static void scheduleReconnect() {
        scheduler.schedule(JavaTraderApplication::connectWithRetry, 10, TimeUnit.SECONDS);
    }
}
