package com.tavisca.affinionds;


import com.tavisca.affinionds.configurations.KafkaConfiguration;
import com.tavisca.affinionds.constants.KafkaConstants;
import com.tavisca.affinionds.utils.JsonToAvroConverter;
import com.tavisca.affinionds.utils.MessageInput;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import parquet.org.codehaus.jackson.JsonNode;
import parquet.org.codehaus.jackson.map.ObjectMapper;

import java.io.InputStream;
import java.util.Properties;

public class AvroSerializerDemo {
    public static void main(String[] args) {

        String inputJson = MessageInput.GetMessageInputInJsonString();
        Schema avroSchema = JsonToAvroConverter.ConvertJsonDataToAvroSchema(inputJson, KafkaConstants.Topic);
        GenericData.Record avroRecord = JsonToAvroConverter.ConvertJsonDataToAvroRecord(inputJson,avroSchema);
        ProducerRecord<String,GenericRecord> producerRecord = new ProducerRecord<String,GenericRecord>(KafkaConstants.Topic,avroRecord);

        Properties configurations = KafkaConfiguration.GetKafkaConfigurations();
        KafkaProducer<String, GenericRecord> kafkaProducer = new KafkaProducer<String, GenericRecord>(configurations);
        kafkaProducer.send(producerRecord);
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}


