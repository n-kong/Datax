package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/1/11
 */
public class RedisProxy {

    private Logger logger = LoggerFactory.getLogger(RedisProxy.class);
    private static Pattern PATTERN = Pattern.compile("\t|\r|\n");
    private RecordSender recordSender = null;
    private Configuration configuration = null;
    public RedisProxy(RecordSender recordSender, Configuration configuration) {
        this.recordSender = recordSender;
        this.configuration = configuration;
    }

    public static void checkNecessaryConfig(Configuration originalConfig) {
        originalConfig.getNecessaryValue(Key.HOST, RedisReaderErrorCode.REQUIRED_VALUE);

        originalConfig.getNecessaryValue(Key.PORT, RedisReaderErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.MATCH_KEY, RedisReaderErrorCode.REQUIRED_VALUE);

        if (null == originalConfig.getList(Key.COLUMN) ||
            originalConfig.getList(Key.COLUMN, String.class).isEmpty()) {
            throw DataXException.asDataXException(RedisReaderErrorCode.REQUIRED_VALUE, "datasphere获取不到源表的列信息， 由于您未配置读取源头表的列信息. datasphere无法知道该抽取表的哪些字段的数据 " +
                "正确的配置方式是给 column 配置上您需要读取的列名称,用英文逗号分隔.");
        }
    }

    public void doRead(String redisData) {
        if (StringUtils.isBlank(redisData)) {
            return;
        }
        List<String> columns = formatColumn(redisData);
        Record record = recordSender.createRecord();
        try {
            JSONObject jsonObject = JSON.parseObject(redisData);
            for (String column : columns) {
                String value = jsonObject.getString(column);
                record.addColumn(new StringColumn(trim(value)));
            }
            recordSender.sendToWriter(record);
        } catch (Exception e) {
            logger.error("redis data to dataSphere date error. error msg is:{}", e.getMessage(), e);
        }
    }

    private List<String> formatColumn(String data) {
        List<String> useColumns = this.configuration.getList(Key.COLUMN, String.class);
        if (1 == useColumns.size() && "*".equals(useColumns.get(0))) {
            Configuration from = Configuration.from(formatData(data));
            Set<String> keys = from.getKeys();
            useColumns = new ArrayList<String>(keys);
        }
        return useColumns;
    }

    private static JSONObject formatData(String data) {
        LinkedHashMap linkedHashMap = JSON.parseObject(data, LinkedHashMap.class, Feature.OrderedField);
        JSONObject jsonObject = new JSONObject(true);
        jsonObject.putAll(linkedHashMap);
        return jsonObject;
    }

    /**
     * 剔除特殊字符
     * @param inStr
     * @return
     */
    public static String trim(String inStr) {
        String outStr = "";
        if (StringUtils.isNotBlank(inStr)) {
            Matcher m = PATTERN.matcher(inStr);
            outStr = m.replaceAll("");
        }
        return outStr.trim();
    }
}
