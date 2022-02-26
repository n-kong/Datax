package com.alibaba.datax.plugin.writer.bkwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/2/2
 */
public enum BkWriterErrorCode implements ErrorCode{
    CONFIG_INVALID_EXCEPTION("BkWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("BkWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("BkWriter-02", "您填写的参数值不合法."),
    Write_ERROR("BkWriter-03", "写入目标库失败.");

    private final String code;
    private final String description;

    private BkWriterErrorCode(String code, String description) {
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
        return String.format("Code:[%s], Description:[%s].", this.code,
            this.description);
    }
}
