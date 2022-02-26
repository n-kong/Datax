package com.zcreate.datax.plugin.writer.kafkawriter;

public enum WriteType {
    JSON("json"),
    TEXT("text");

    private String name;

    WriteType(String name) {
        this.name = name;
    }
}
