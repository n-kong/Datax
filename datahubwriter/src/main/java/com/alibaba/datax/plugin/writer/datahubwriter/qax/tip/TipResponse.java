package com.alibaba.datax.plugin.writer.datahubwriter.qax.tip;

public class TipResponse {

    private String accessToken;

    private String consumerId;

    private String consumerName;

    private Integer expiresIn;

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    public Integer getExpiresIn() {
        return expiresIn;
    }

    public void setExpiresIn(Integer expiresIn) {
        this.expiresIn = expiresIn;
    }

    @Override
    public String toString() {
        return "TipResponse{" +
                "accessToken='" + accessToken + '\'' +
                ", consumerId='" + consumerId + '\'' +
                ", consumerName='" + consumerName + '\'' +
                ", expiresIn='" + expiresIn + '\'' +
                '}';
    }
}
