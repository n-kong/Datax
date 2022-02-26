package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/5/25
 */
public enum ESReaderErrorCode implements ErrorCode {
    NULL_CONFIG_VALUE("ESReader-06", "该配置项不允许为空，请您重新配置."),
    BAD_CONFIG_VALUE("ESReader-00", "您配置的值不合法."),
    ES_INDEX_DELETE("ESReader-01", "删除index错误."),
    ES_INDEX_CREATE("ESReader-02", "创建index错误."),
    ES_MAPPINGS("ESReader-03", "mappings错误."),
    ES_INDEX_INSERT("ESReader-04", "插入数据错误."),
    ES_ALIAS_MODIFY("ESReader-05", "别名修改错误."),
    ;

    private final String code;
    private final String description;

    ESReaderErrorCode(String code, String description) {
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
