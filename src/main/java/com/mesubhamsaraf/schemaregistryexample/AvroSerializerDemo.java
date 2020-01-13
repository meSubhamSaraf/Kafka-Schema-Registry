package com.mesubhamsaraf.schemaregistryexample;


import com.mesubhamsaraf.schemaregistryexample.constants.KafkaConstants;
import com.mesubhamsaraf.schemaregistryexample.utils.JsonToAvroConverter;
import com.mesubhamsaraf.schemaregistryexample.configurations.KafkaConfiguration;
import com.mesubhamsaraf.schemaregistryexample.utils.MessageInput;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class AvroSerializerDemo {
    public static void main(String[] args) {

        String inputJson = MessageInput.GetMessageInputInJsonString();
        Schema avroSchema = JsonToAvroConverter.ConvertJsonDataToAvroSchema(inputJson, KafkaConstants.TOPIC);
        GenericData.Record avroRecord = JsonToAvroConverter.ConvertJsonDataToAvroRecord(inputJson,avroSchema);
        ProducerRecord<String,GenericRecord> producerRecord = new ProducerRecord<String,GenericRecord>(KafkaConstants.TOPIC,avroRecord);

        Properties configurations = KafkaConfiguration.GetKafkaConfigurations();
        KafkaProducer<String, GenericRecord> kafkaProducer = new KafkaProducer<String, GenericRecord>(configurations);
        kafkaProducer.send(producerRecord);
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}


