package com.example.javatrader.start;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Timer;
import java.util.TimerTask;

public class DhanWebSocketClient extends WebSocketClient {

    private final DhanWebSocketListener listener;
    private Timer heartbeatTimer;

    public DhanWebSocketClient(URI serverUri, DhanWebSocketListener listener) {
        super(serverUri);
        this.listener = listener;
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        System.out.println("Connected to DhanHQ WebSocket");
        subscribeToInstruments();
        startHeartbeat();
        listener.onConnected();
    }

    @Override
    public void onMessage(String message) {
        System.out.println("Received text message: " + message);
    }

    @Override
    public void onMessage(ByteBuffer bytes) {
        parseBinaryData(bytes);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("Connection closed. Reason: " + reason);
        stopHeartbeat();
        listener.onDisconnected();
    }

    @Override
    public void onError(Exception ex) {
        System.err.println("An error occurred: " + ex.getMessage());
        listener.onError(ex);
    }

    private void subscribeToInstruments() {
        String subscriptionMessage = "{\"action\":\"subscribe\",\"instruments\":[{\"exchangeSegment\":\"NSE_EQ\",\"instrumentId\":\"1330\"}]}";
        send(subscriptionMessage);
    }

    private void startHeartbeat() {
        heartbeatTimer = new Timer();
        heartbeatTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                send("ping");
            }
        }, 0, 30000);
    }

    private void stopHeartbeat() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel();
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
            System.err.println("Failed to parse binary data: " + e.getMessage());
        }
    }
}