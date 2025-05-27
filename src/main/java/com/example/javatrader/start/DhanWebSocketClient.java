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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DhanWebSocketClient extends WebSocketClient {
    private static final Logger logger = LoggerFactory.getLogger(DhanWebSocketClient.class);

    private final DhanWebSocketListener listener;
    private Timer heartbeatTimer;
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
    private final BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
    private static final int MAX_RECONNECT_ATTEMPTS = 5;
    private static final long INITIAL_RECONNECT_DELAY_MS = 1000;
    private static final long MAX_RECONNECT_DELAY_MS = 60000;
    private static final long HEARTBEAT_INTERVAL_MS = 30000;
    private static final long RATE_LIMIT_DELAY_MS = 60000;
    private static final long MESSAGE_DELAY_MS = 150;
    private final AtomicBoolean reconnectInProgress = new AtomicBoolean(false);
    private final AtomicBoolean rateLimited = new AtomicBoolean(false);

    public DhanWebSocketClient(URI serverUri, DhanWebSocketListener listener) {
        super(serverUri);
        this.listener = listener;
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        reconnectAttempts.set(0);
        reconnectInProgress.set(false);
        rateLimited.set(false);
        logger.info("Connected to DhanHQ WebSocket");
        processQueuedMessages();
        startHeartbeat();
        listener.onConnected();
    }

    private void processQueuedMessages() {
        new Thread(() -> {
            while (!messageQueue.isEmpty() && isOpen()) {
                try {
                    String message = messageQueue.take();
                    this.send(message);
                    logger.debug("Sent queued message: {}", message);
                    Thread.sleep(MESSAGE_DELAY_MS); // prevent spamming
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

        // Handle 429 Too Many Requests with special delay
        if (reason != null && reason.contains("429")) {
            rateLimited.set(true);
            logger.warn("Received 429 Too Many Requests. Delaying reconnect by {} seconds.", RATE_LIMIT_DELAY_MS/1000);
            scheduleReconnect(RATE_LIMIT_DELAY_MS);
        } else {
            rateLimited.set(false);
            attemptReconnect();
        }
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
//            attemptReconnect();
            return;
        }

        try {
            // Add extra delay if we're recovering from rate limiting
            if (rateLimited.get()) {
                Thread.sleep(RATE_LIMIT_DELAY_MS);
                rateLimited.set(false);
            } else {
                Thread.sleep(MESSAGE_DELAY_MS);
            }

            send(message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Error while sending message", e);
            messageQueue.add(message); // retry later
//            attemptReconnect();
        }
    }

    private void attemptReconnect() {
        synchronized (reconnectInProgress) {
            if (isOpen() || reconnectInProgress.get()) {
                return;
            }

            if (reconnectAttempts.incrementAndGet() <= MAX_RECONNECT_ATTEMPTS) {
                reconnectInProgress.set(true);
                long delay = calculateReconnectDelay();
                logger.info("Scheduling reconnect attempt {} in {} ms", reconnectAttempts.get(), delay);
                scheduleReconnect(delay);
            } else {
                logger.error("Max reconnection attempts reached. Giving up.");
                reconnectAttempts.set(0); // Reset for next time
            }
        }
    }

    private long calculateReconnectDelay() {
        if (rateLimited.get()) {
            return RATE_LIMIT_DELAY_MS;
        }
        return Math.min(
                INITIAL_RECONNECT_DELAY_MS * (1L << (reconnectAttempts.get() - 1)),
                MAX_RECONNECT_DELAY_MS
        );
    }

    private void scheduleReconnect(long delayMs) {
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    if (!isOpen()) {
                        logger.info("Attempting reconnect...");
                        reconnect();
                    }
                } catch (Exception e) {
                    logger.error("Reconnect failed", e);
                    attemptReconnect(); // try again
                } finally {
                    reconnectInProgress.set(false);
                }
            }
        }, delayMs);
    }

    private void startHeartbeat() {
        heartbeatTimer = new Timer();
        heartbeatTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    if (isOpen()) {
                        sendWithGuarantee("ping");
                        logger.debug("Sent heartbeat");
                    }
                } catch (Exception e) {
                    logger.error("Failed to send heartbeat", e);
                }
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS);
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