package org.itmo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class MessageSerializer {
    private static final Gson gson = new GsonBuilder().create();
    
    public static byte[] serialize(TaskMessage message) {
        String json = gson.toJson(message);
        return json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
    
    public static TaskMessage deserializeTask(byte[] data) {
        String json = new String(data, java.nio.charset.StandardCharsets.UTF_8);
        return gson.fromJson(json, TaskMessage.class);
    }
    
    public static byte[] serialize(ResultMessage message) {
        String json = gson.toJson(message);
        return json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
    
    public static ResultMessage deserializeResult(byte[] data) {
        String json = new String(data, java.nio.charset.StandardCharsets.UTF_8);
        return gson.fromJson(json, ResultMessage.class);
    }
}

