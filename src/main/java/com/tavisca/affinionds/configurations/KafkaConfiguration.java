package com.tavisca.affinionds.configurations;

import com.tavisca.affinionds.constants.KafkaConstants;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaConfiguration{
    public static Properties GetKafkaConfigurations(){

        Properties configProperties = new Properties();
        try (InputStream input = KafkaConfiguration.class.getClassLoader().getResourceAsStream(KafkaConstants.KafkaProperties)) {
            configProperties.load(input);
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }

        Properties configurations = new Properties();
        configurations.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configProperties.getProperty(KafkaConstants.BootStrapServers));
        configurations.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        configurations.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        configurations.put(KafkaConstants.SchemaRegistryUrl,configProperties.getProperty(KafkaConstants.SchemaRegistryUrl));
        return configurations;
    }
}
