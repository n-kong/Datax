package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchScroll;
import io.searchbox.params.Parameters;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/5/25
 */
public class ESReader extends Reader{

    public static class Job extends Reader.Job {

        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;
        private ESUtils esUtils = null;

        @Override
        public void init() {
            this.conf = this.getPluginJobConf();
            conf.getNecessaryValue(Key.ENDPOINT, ESReaderErrorCode.NULL_CONFIG_VALUE);
            conf.getNecessaryValue(Key.INDEX, ESReaderErrorCode.NULL_CONFIG_VALUE);
            List<String> columns = conf.getList(Key.COLUMN, null, String.class);
            if (columns.isEmpty() || columns.get(0).equalsIgnoreCase("*")){
                throw DataXException.asDataXException(ESReaderErrorCode.BAD_CONFIG_VALUE,
                    String.format("您提供配置文件有误，[%s]参数需要配置详细的列名，不允许为空或为'*'，请您重新配置.", Key.COLUMN));
            }
            String type = conf.getString(Key.TYPE, "");
            if (StringUtils.isBlank(type)) {
                log.warn("您配置的type为空，DataSphere默认会抽取Index为[{}]下的所有数据，如果这不符合您的预期，请您重新配置", conf.getString(Key.INDEX));
                conf.set(Key.TYPE, type);
            }
        }


        @Override
        public void prepare() {
            //esUtils = new ESUtils(conf);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            log.info("ESReader Job start split.");
            List<Configuration> configurations = new ArrayList<Configuration>();
            Configuration tmpConf = this.conf.clone();
            configurations.add(tmpConf);
            log.info("ESReader Job end split.");
            return configurations;
        }

        /**
         * 由于es利用from+size进行分度遍历最多查询10000条数据，大数据环境下不适用，所以弃用，
         * 改用scroll搜索，仅支持单线程滑动搜索
         * @return
         */
        private List<Configuration> tmpSplit() {
            int splitNum = this.conf.getInt(Key.SPLIT_NUM, Content.SPLIT_NUM);
            List<Configuration> configurations = new ArrayList<Configuration>();
            long numTotal = esUtils.queryTotal();
            // 单线程处理情况
            if (1 == splitNum) {
                Configuration tmpConf = this.conf.clone();
                tmpConf.set(Key.START_PAGE, 1);
                tmpConf.set(Key.TOTAL, numTotal);
                configurations.add(tmpConf);
            } else {
                configurations = splitPage(configurations, splitNum, numTotal);
            }
            return configurations;
        }

        private List<Configuration> splitPage(List<Configuration> configurations, int splitNum, long numTotal) {
            int batchSize = this.conf.getInt(Key.BATCH_SIZE);
            // 每个线程处理的消息总数
            long number = numTotal / splitNum;
            // 消息总数与线程数非倍数关系是，线程数+1
            if (numTotal % splitNum > 0) {
                splitNum += 1;
            }
            // 每个线程处理的数据量是否与批处理大小成倍数关系
            long tmp = number % batchSize;
            if (tmp > 0) {
                number = number - tmp;
            }
            // 每个线程处理的消息总数小于用户配置的批处理大小时，将批处理大小调整为计算出的线程处理数据量
            if (number < batchSize) {
                this.conf.set(Key.BATCH_SIZE, number);
            }
            // 分页间隔
            long pageInter = number / batchSize;
            Configuration tmpConf = null;
            for (int i = 0; i < splitNum; i++) {
                tmpConf = this.conf.clone();
                tmpConf.set(Key.START_PAGE, i * pageInter + 1);
                if (i == splitNum - 1) {
                    tmpConf.set(Key.TOTAL, numTotal - number * i);
                } else {
                    tmpConf.set(Key.TOTAL, number);
                }
                configurations.add(tmpConf);
            }
            return configurations;
        }

        @Override
        public void destroy() {
        }
    }


    public static class Task extends Reader.Task {

        private static final Logger log = LoggerFactory.getLogger(Task.class);

        private Configuration conf = null;
        private String index = null;
        private String type = null;
        private Map<String, Object> matchValue = null;
        private List<String> columns = null;
        private int batchSize = 1024;
        private ESUtils esUtils = null;

        @Override
        public void init() {

            conf = this.getPluginJobConf();

            // es index，非空
            index = conf.getString(Key.INDEX);
            // es type
            type = conf.getString(Key.TYPE);
            // 根据指定字段值搜索
            matchValue = conf.getMap(Key.MATCH_KEY, null,null);
            columns = conf.getList(Key.COLUMN, String.class);
            batchSize = conf.getInt(Key.BATCH_SIZE, Content.BATCH_SIZE);

        }

        @Override public void prepare() {
            this.esUtils = new ESUtils(conf);
        }

        @Override
        public void startRead(RecordSender recordSender) {

            // es 客户端
            JestClient jestClient = esUtils.getClient();
            // 构建搜索项
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
            if (null != matchValue) {
                Set<Map.Entry<String, Object>> entries = matchValue.entrySet();
                Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Object> data = iterator.next();
                    queryBuilder.must(QueryBuilders.matchQuery(data.getKey(), data.getValue()));
                }
            }
            // 只返回指定列
            searchSourceBuilder.fetchSource(columns.toArray(new String[]{}), null);
            searchSourceBuilder.query(queryBuilder);
            log.info("线程-{},即将读取的数据总量为：{}.", Thread.currentThread().getName(), esUtils.queryTotal());
            Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(index)
                .addType(type)
                .setParameter(Parameters.SIZE, batchSize)
                .setParameter(Parameters.SCROLL, "1m")
                .build();
            try {
                JestResult jestResult = jestClient.execute(search);
                while (jestResult.isSucceeded() && jestResult.getSourceAsStringList().size() > 0) {
                    List<JSONObject> jsonObjects = jestResult.getSourceAsObjectList(JSONObject.class);
                    for (JSONObject jsonObject : jsonObjects) {
                        Record record = recordSender.createRecord();
                        for (String column : columns) {
                            String value = jsonObject.getString(column);
                            record.addColumn(new StringColumn(conf.trim(value)));
                        }
                        recordSender.sendToWriter(record);
                    }
                    String scrollId = jestResult.getJsonObject().get("_scroll_id").getAsString();
                    SearchScroll searchScroll = new SearchScroll.Builder(scrollId, "1m").build();
                    jestResult = jestClient.execute(searchScroll);
                }
            } catch (IOException e) {
                log.error("Es query error. msg:{}", e.getMessage(), e);

            }
        }

        private void reader(RecordSender recordSender) {
            // es 客户端
            JestClient jestClient = esUtils.getClient();
            // 构建搜索项
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
            if (null != matchValue) {
                Set<Map.Entry<String, Object>> entries = matchValue.entrySet();
                Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Object> data = iterator.next();
                    queryBuilder.must(QueryBuilders.matchQuery(data.getKey(), data.getValue()));
                }
            }
            // 只返回指定列
            searchSourceBuilder.fetchSource(columns.toArray(new String[]{}), null);
            searchSourceBuilder.query(queryBuilder);
            // 分页累加器
            int page = this.conf.getInt(Key.START_PAGE);
            long total = this.conf.getLong(Key.TOTAL);
            log.info("线程-{},即将读取的数据总量为：{}.", Thread.currentThread().getName(), total);
            long status = 0;
            while (true) {
                searchSourceBuilder.from((page - 1) * batchSize).size(batchSize);
                Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(index).addType(type).build();
                JestResult jestResult = null;
                try {
                    jestResult = jestClient.execute(search);
                    if (!jestResult.isSucceeded()) {
                        break;
                    }
                    JSONObject jsonObject = JSONObject.parseObject(jestResult.getJsonString());
                    JSONArray jsonArray = jsonObject.getJSONObject("hits").getJSONArray("hits");
                    status += jsonArray.size();
                    if (jsonArray.size() == 0 || status > total) {
                        break;
                    }
                    for (int i = 0; i < jsonArray.size(); i++) {
                        JSONObject data = jsonArray.getJSONObject(i).getJSONObject("_source");
                        Set<Map.Entry<String, Object>> entries = data.entrySet();
                        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();

                        Record record = recordSender.createRecord();
                        while (iterator.hasNext()) {
                            Map.Entry<String, Object> next = iterator.next();
                            String value = String.valueOf(next.getValue());
                            record.addColumn(new StringColumn(conf.trim(value)));
                        }
                        recordSender.sendToWriter(record);
                    }
                    page ++;
                } catch (IOException e) {
                    log.error("Es query error. msg:{}", e.getMessage(), e);
                }
            }
        }

        @Override
        public void destroy() {
            esUtils.closeJestClient();
        }
    }

}
