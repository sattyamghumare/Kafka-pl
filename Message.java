package com.example.kafkaspark.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message implements Serializable {
    private String id;
    private String content;
    private String userId;
    private Long timestamp;
    private String category;
    
    public static Message createSample(String id, String content, String userId, String category) {
        Message msg = new Message();
        msg.setId(id);
        msg.setContent(content);
        msg.setUserId(userId);
        msg.setTimestamp(System.currentTimeMillis());
        msg.setCategory(category);
        return msg;
    }
    
    public String toJson() {
        return String.format("{\"id\":\"%s\",\"content\":\"%s\",\"userId\":\"%s\",\"timestamp\":%d,\"category\":\"%s\"}",
                id, content, userId, timestamp, category);
    }
}
