package com.alibaba.datax.plugin.writer.datahubwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.aliyun.datahub.DatahubClient;
import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.Account;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.RecordSchema;
import java.util.ArrayList;
import java.util.List;

public class DatahubUtil {

    public static int MAX_RETRY_TIME = 10;

    public static void checkNecessaryConfig(Configuration originalConfig) {

        // 检查datahub配置是否正确
        originalConfig.getNecessaryValue(Key.DATAHUB_ENDPOINT,
            DatahubWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PROJECT,
            DatahubWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.TOPIC,
            DatahubWriterErrorCode.REQUIRED_VALUE);

        if (null == originalConfig.getList(Key.SCHEMA) ||
            originalConfig.getList(Key.SCHEMA, String.class).isEmpty()) {
            throw DataXException
                .asDataXException(DatahubWriterErrorCode.REQUIRED_VALUE, "您未配置写入Datahub目的Topic的列信息. " +
                    "正确的配置方式是给datax的 column 项配置上您需要读取的列名称,用英文逗号分隔 例如:  \"column\": [\"id\",\"name\"].");
        }

    }

    public static void dealMaxRetryTime(Configuration originalConfig) {
        int maxRetryTime = originalConfig.getInt(Key.MAX_RETRY_TIME,
            DatahubUtil.MAX_RETRY_TIME);
        if (maxRetryTime < 1 || maxRetryTime > DatahubUtil.MAX_RETRY_TIME) {
            throw DataXException.asDataXException(DatahubWriterErrorCode.ILLEGAL_VALUE, "您所配置的maxRetryTime 值错误. 该值不能小于1, 且不能大于 " + DatahubUtil.MAX_RETRY_TIME +
                ". 推荐的配置方式是给maxRetryTime 配置1-11之间的某个值. 请您检查配置并做出相应修改.");
        }
        MAX_RETRY_TIME = maxRetryTime;
    }

    public static DatahubClient initDatahubClient(Configuration originalConfig) {
        String accountType = originalConfig.getString(Key.ACCOUNT_TYPE);
        String accessId = originalConfig.getString(Key.ACCESS_ID);
        String accessKey = originalConfig.getString(Key.ACCESS_KEY);
        String datahubServer = originalConfig.getString(Key.DATAHUB_ENDPOINT);

        Account account;
        if (accountType.equalsIgnoreCase(Constant.DEFAULT_ACCOUNT_TYPE)) {
            account = new AliyunAccount(accessId, accessKey);
        } else {
            throw DataXException.asDataXException(DatahubWriterErrorCode.ACCOUNT_TYPE_ERROR,
                String.format("不支持的账号类型:[%s]. 账号类型目前仅支持aliyun, taobao.", accountType));
        }
        DatahubConfiguration datahubConfiguration =
            new DatahubConfiguration(account, datahubServer);
        datahubConfiguration.setSocketTimeout(60000);
        datahubConfiguration.setSocketConnectTimeout(60000);
        DatahubClient client = new DatahubClient(datahubConfiguration);
        return client;
    }

    public static List<String> getAllSchema(RecordSchema schema) {
        if (null == schema) {
            throw new IllegalArgumentException("parameter schema can not be null.");
        }
        List<String> allColumns = new ArrayList<String>();
        List<Field> fields = schema.getFields();
        for (Field field : fields) {
            allColumns.add(field.getName());
        }
        return allColumns;
    }

    public static List<Integer> parsePosition(List<String> allColumnList,
        List<String> userConfiguredColumns) {
        List<Integer> retList = new ArrayList<Integer>();

        boolean hasColumn;
        for (String col : userConfiguredColumns) {
            hasColumn = false;
            for (int i = 0, len = allColumnList.size(); i < len; i++) {
                if (allColumnList.get(i).equalsIgnoreCase(col)) {
                    retList.add(i);
                    hasColumn = true;
                    break;
                }
            }
            if (!hasColumn) {
                throw DataXException.asDataXException(DatahubWriterErrorCode.COLUMN_NOT_EXIST,
                    String.format("Datahub目的Topic的列配置错误. 由于您所配置的列:%s 不存在，会导致datax无法正常写入数据，请检查该列是否存在，如果存在请检查大小写等配置.", col));
            }
        }
        return retList;
    }



}
