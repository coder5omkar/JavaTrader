package com.example.javatrader;

import com.example.javatrader.model.OrderStatusMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderStatusConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "order-status", groupId = "order-status-group")
    public void consumeOrderStatus(ConsumerRecord<String, String> record) {
        try {
            String message = record.value();
            OrderStatusMessage status = objectMapper.readValue(message, OrderStatusMessage.class);
            System.out.println("✅ Received Order Status: " + status);

            // You can now store this in DB or trigger further logic here

        } catch (Exception e) {
            System.err.println("❌ Error processing Kafka message: " + e.getMessage());
        }
    }
}
