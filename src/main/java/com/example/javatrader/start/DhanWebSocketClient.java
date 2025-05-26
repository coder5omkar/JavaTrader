package com.example.javatrader.start;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class DhanWebSocketClient extends WebSocketClient {
    private static final Logger logger = LoggerFactory.getLogger(DhanWebSocketClient.class);

    private final DhanWebSocketListener listener;
    private Timer heartbeatTimer;
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
    private final BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
    private static final int MAX_RECONNECT_ATTEMPTS = 5;
    private static final long INITIAL_RECONNECT_DELAY_MS = 1000;
    private static final long HEARTBEAT_INTERVAL_MS = 30000;

    public DhanWebSocketClient(URI serverUri, DhanWebSocketListener listener) {
        super(serverUri);
        this.listener = listener;
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        reconnectAttempts.set(0);
        logger.info("Connected to DhanHQ WebSocket");
        processQueuedMessages();
        startHeartbeat();
        listener.onConnected();
    }

    private void processQueuedMessages() {
        new Thread(() -> {
            while (!messageQueue.isEmpty()) {
                try {
                    String message = messageQueue.take();
                    this.send(message);
                    logger.debug("Sent queued message: {}", message);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Failed to send queued message", e);
                }
            }
        }).start();
    }

    @Override
    public void onMessage(String message) {
        logger.debug("Received text message: {}", message);
        listener.onTextMessage(message);
    }

    @Override
    public void onMessage(ByteBuffer bytes) {
        try {
            parseBinaryData(bytes);
        } catch (Exception e) {
            logger.error("Error parsing binary message", e);
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        logger.warn("Connection closed. Code: {}, Reason: {}, Remote: {}", code, reason, remote);
        stopHeartbeat();
        listener.onDisconnected();
        attemptReconnect();
    }

    @Override
    public void onError(Exception ex) {
        logger.error("WebSocket error", ex);
        listener.onError(ex);
    }

    public void sendWithGuarantee(String message) {
        if (!isOpen()) {
            messageQueue.add(message);
            logger.debug("Queued message while disconnected");
            if (reconnectAttempts.get() == 0) {
                attemptReconnect();
            }
        } else {
            send(message);
        }
    }

    private void attemptReconnect() {
        if (reconnectAttempts.incrementAndGet() <= MAX_RECONNECT_ATTEMPTS) {
            long delay = INITIAL_RECONNECT_DELAY_MS * (long) Math.pow(2, reconnectAttempts.get() - 1);
            logger.info("Scheduling reconnect attempt {} in {} ms", reconnectAttempts.get(), delay);
            new Timer().schedule(new TimerTask() {
                @Override
                public void run() {
                    reconnect();
                }
            }, delay);
        } else {
            logger.error("Max reconnection attempts reached");
        }
    }

    private void startHeartbeat() {
        heartbeatTimer = new Timer();
        heartbeatTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                sendWithGuarantee("ping");
                logger.debug("Sent heartbeat");
            }
        }, 0, HEARTBEAT_INTERVAL_MS);
    }

    private void stopHeartbeat() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel();
            heartbeatTimer = null;
        }
    }

    private void parseBinaryData(ByteBuffer buffer) {
        try {
            buffer.order(ByteOrder.BIG_ENDIAN);
            buffer.position(8); // Skip header
            float ltp = buffer.getFloat();
            int ltt = buffer.getInt();
            listener.onDataReceived(ltp, ltt);
        } catch (Exception e) {
            logger.error("Failed to parse binary data", e);
            throw new RuntimeException("Binary data parsing failed", e);
        }
    }
}