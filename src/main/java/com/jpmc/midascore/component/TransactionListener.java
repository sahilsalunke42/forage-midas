package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void listen(String transactionLine) {
        Transaction transaction = deserialize(transactionLine);
        transaction.toString();
    }

    private Transaction deserialize(String transactionLine) {
        String[] parts = transactionLine.split(",");
        long senderId = Long.parseLong(parts[0].trim());
        long recipientId = Long.parseLong(parts[1].trim());
        float amount = Float.parseFloat(parts[2].trim());
        return new Transaction(senderId, recipientId, amount);
    }
}