package com.poc.kstreams.merge;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonMessageMapper {

    private  static final ObjectMapper mapper = new ObjectMapper();

    public static Message map(String s){
        try {
            return mapper.readValue(s, Message.class);
        }catch (Exception e){
            System.out.println("Errors converting to message : " + s);
            return null;
        }
    }
}
