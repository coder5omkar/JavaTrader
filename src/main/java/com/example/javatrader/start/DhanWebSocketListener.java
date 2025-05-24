package com.example.javatrader.start;

public interface DhanWebSocketListener {
    void onConnected();
    void onDisconnected();
    void onError(Exception ex);
    void onDataReceived(float ltp, int ltt);
}
