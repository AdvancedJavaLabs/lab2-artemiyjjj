package org.itmo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public abstract class KafkaProducerBase {
    protected final KafkaProducer<String, byte[]> producer;
    
    public KafkaProducerBase(KafkaProducer<String, byte[]> sharedProducer) {
        this.producer = sharedProducer;
    }
    
    public void produce(String topic, String key, byte[] message) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, message);
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("[KafkaProducer] Error sending message to topic " + topic + ": " + exception.getMessage());
                    exception.printStackTrace();
                } else {
                    System.out.println("[KafkaProducer] Published message to topic: " + topic + 
                                     " partition: " + metadata.partition() + 
                                     " offset: " + metadata.offset() + 
                                     " (size: " + message.length + " bytes)");
                }
            });
        } catch (Exception e) {
            System.err.println("[KafkaProducer] Exception while sending: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

