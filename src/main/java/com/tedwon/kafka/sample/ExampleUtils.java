package com.tedwon.kafka.sample;

import kafka.message.Message;

import java.nio.ByteBuffer;

public class ExampleUtils {
    public static String getMessage(Message message) {
        ByteBuffer buffer = message.payload();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes);
    }
}
