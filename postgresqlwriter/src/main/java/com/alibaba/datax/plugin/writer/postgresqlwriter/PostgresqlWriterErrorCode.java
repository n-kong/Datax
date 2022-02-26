package com.alibaba.datax.plugin.writer.postgresqlwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/7/20
 */
public enum PostgresqlWriterErrorCode implements ErrorCode{
    REQUIRED_VALUE("PostgresqlWriter-00", "请优化配置参数.");

    private final String code;
    private final String description;


    private PostgresqlWriterErrorCode(String code, String description) {
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

}
