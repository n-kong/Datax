package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum KafkaReaderErrorCode implements ErrorCode {
    TOPIC_ERROR("KafkaReader-00", "没有设置Kafka topic"),
    ADDRESS_ERROR("KafkaReader-01", "没有设置kafka.broker地址"),
    PARTITION_ERROR("KafkaReader-02", "没有设置kafka topic 的分区数量"),
    KAFKA_READER_ERROR("KafkaReader-03", "kafka reader 错误"),;
    private final String code;
    private final String description;

    private KafkaReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
