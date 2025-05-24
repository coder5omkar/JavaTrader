package com.example.javatrader.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;

@Entity
@Table(name = "ticker_data")
public class TickerData {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private float ltp;

    private int ltt;  // timestamp from feed (epoch seconds)

    private LocalDateTime receivedAt;

    public TickerData() {}

    public TickerData(float ltp, int ltt, LocalDateTime receivedAt) {
        this.ltp = ltp;
        this.ltt = ltt;
        this.receivedAt = receivedAt;
    }

    // Getters and setters omitted for brevity

    public Long getId() {
        return id;
    }

    public float getLtp() {
        return ltp;
    }

    public void setLtp(float ltp) {
        this.ltp = ltp;
    }

    public int getLtt() {
        return ltt;
    }

    public void setLtt(int ltt) {
        this.ltt = ltt;
    }

    public LocalDateTime getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(LocalDateTime receivedAt) {
        this.receivedAt = receivedAt;
    }
}
