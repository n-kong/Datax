package com.alibaba.datax.plugin.reader.datahubreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum DatahubReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("DatahubReader-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("DatahubReader-01", "您配置的值不合法."),

    GET_ID_KEY_FAIL("OdpsWriter-06", "获取 accessId/accessKey 失败."),

    WRITER_RECORD_FAIL("DatahubWriter-09", "写入数据到 Datahub 目的Topic失败."),



    ACCOUNT_TYPE_ERROR("DatahubWriter-30", "账号类型错误."),

    TOPIC_VALUE("DatahubReader-31", "Topic不存在或配置有误"),


    COLUMN_NOT_EXIST("DatahubWriter-32", "用户配置的列不存在."),

    ODPS_ACCESS_KEY_ID_NOT_FOUND("OdpsWriter-102", "您配置的值不合法, odps accessId,accessKey 不存在"), //ODPS-0410051:Invalid credentials - accessKeyId not found

    ODPS_ACCESS_KEY_INVALID("OdpsWriter-103", "您配置的值不合法, odps accessKey 错误"), //ODPS-0410042:Invalid signature value - User signature dose not match;

    ODPS_ACCESS_DENY("OdpsWriter-104", "拒绝访问, 您不在 您配置的 project 中") //ODPS-0420095: Access Denied - Authorization Failed [4002], You doesn't exist in project

    ;

    private final String code;
    private final String description;

    private DatahubReaderErrorCode(String code, String description) {
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
