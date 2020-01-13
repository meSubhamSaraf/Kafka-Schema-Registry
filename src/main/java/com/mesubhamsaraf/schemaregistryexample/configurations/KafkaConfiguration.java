package com.mesubhamsaraf.schemaregistryexample.configurations;

import com.mesubhamsaraf.schemaregistryexample.constants.KafkaConstants;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaConfiguration{
    public static Properties GetKafkaConfigurations(){

        Properties configProperties = new Properties();
        try (InputStream input = KafkaConfiguration.class.getClassLoader().getResourceAsStream(KafkaConstants.KAFKA_PROPERTIES)) {
            configProperties.load(input);
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }

        Properties configurations = new Properties();
        configurations.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configProperties.getProperty(KafkaConstants.BOOT_STRAP_SERVERS));
        configurations.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        configurations.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        configurations.put(KafkaConstants.SCHEMA_REGISTRY_URL,configProperties.getProperty(KafkaConstants.SCHEMA_REGISTRY_URL));
        return configurations;
    }
}
