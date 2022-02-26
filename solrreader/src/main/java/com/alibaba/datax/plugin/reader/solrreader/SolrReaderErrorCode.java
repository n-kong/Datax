package com.alibaba.datax.plugin.reader.solrreader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/5/20
 */
public enum SolrReaderErrorCode implements ErrorCode {

    REQUIRED_VALUE("SolrReader-00", "读取Solr数据时出现错误."),
    NOTNULL_VALUE("SolrReader-01", "您缺少必填参数."),
    MIXED_INDEX_VALUE("TxtFileReader-02", "您的列信息配置同时包含了index,value."),;

    private final String code;
    private final String description;

    private SolrReaderErrorCode(String code, String description) {
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
