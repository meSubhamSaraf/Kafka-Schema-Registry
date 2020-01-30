package com.mesubhamsaraf.kafka.twitter.poc;

import com.mesubhamsaraf.schemaregistryexample.constants.KafkaConstants;
import com.twitter.hbc.core.Client;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    public static void main(String[] args) {
        System.out.println("hello twitter");
        Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        //create twitter client
        Client client = new Twitter().CreateClient(msgQueue);

        //Connect to client
        client.connect();
        //create kafka producer
        KafkaProducer<String, String> producer = CreateKafkaProducer();

        //loop through the process
        while (!client.isDone()) {
            String msg = null;
            try{
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e){
                e.printStackTrace();
                client.stop();
            }
            if(msg != null){
                logger.info(msg);
                producer.send(new ProducerRecord<>("tweets_realtime", msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        logger.info("someting went wrong", e);
                    }
                });
            }
        }
        logger.info("done");
    }

    private static KafkaProducer<String, String> CreateKafkaProducer() {
        Properties configurations = new Properties();
        configurations.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "subham-virtualbox:9092");
        configurations.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        configurations.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());

        return new KafkaProducer<String, String>(configurations);
    }
}
