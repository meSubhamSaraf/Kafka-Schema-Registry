package com.mesubhamsaraf.schemaregistryexample.utils;

import com.mesubhamsaraf.schemaregistryexample.constants.KafkaConstants;
import parquet.org.codehaus.jackson.JsonNode;
import parquet.org.codehaus.jackson.map.ObjectMapper;

import java.io.InputStream;

public class MessageInput {
    public static String GetMessageInputInJsonString(){
        try(InputStream inputStream =Thread.currentThread().getContextClassLoader().getResourceAsStream(KafkaConstants.MESSAGE_INPUT_JSON)){
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readValue(inputStream ,
                    JsonNode.class);
            return mapper.writeValueAsString(jsonNode);
        }
        catch(Exception e){
            throw new RuntimeException(e);
        }
    }
}
