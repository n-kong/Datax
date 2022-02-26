package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/1/11
 */
public enum  RedisReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("RedisReader-00", "您缺失了必须填写的参数值."),;
    private final String code;
    private final String description;

    private RedisReaderErrorCode(String code, String description) {
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
