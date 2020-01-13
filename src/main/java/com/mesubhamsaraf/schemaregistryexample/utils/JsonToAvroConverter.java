package com.mesubhamsaraf.schemaregistryexample.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.kitesdk.data.spi.JsonUtil;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

public class JsonToAvroConverter{
    public static Schema ConvertJsonDataToAvroSchema(String json, String schemaName){
        return JsonUtil.inferSchema(JsonUtil.parse(json), schemaName);
    }
    public static GenericData.Record ConvertJsonDataToAvroRecord(String json, Schema avroSchema){
        JsonAvroConverter jsonDataToAvroRecordConverter = new JsonAvroConverter();
        GenericData.Record AvroRecord = jsonDataToAvroRecordConverter.convertToGenericDataRecord(json.getBytes(), avroSchema);
        return AvroRecord;
    }
}
