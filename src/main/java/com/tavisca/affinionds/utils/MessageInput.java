package com.tavisca.affinionds.utils;

import com.tavisca.affinionds.constants.KafkaConstants;
import parquet.org.codehaus.jackson.JsonNode;
import parquet.org.codehaus.jackson.map.ObjectMapper;

import java.io.InputStream;

public class MessageInput {
    public static String GetMessageInputInJsonString(){
        try(InputStream inputStream =Thread.currentThread().getContextClassLoader().getResourceAsStream(KafkaConstants.MessageInput)){
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
