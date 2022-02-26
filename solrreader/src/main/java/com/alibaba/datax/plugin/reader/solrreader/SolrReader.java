package com.alibaba.datax.plugin.reader.solrreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/5/20
 */
public class SolrReader extends Reader {

    public static class Job extends Reader.Job {

        private static Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration configuration = null;

        @Override public void init() {
            this.configuration = this.getPluginJobConf();
            configuration.getNecessaryValue(Key.SOLR_URL, SolrReaderErrorCode.NOTNULL_VALUE);
            configuration.getNecessaryValue(Key.COLUMN, SolrReaderErrorCode.NOTNULL_VALUE);
            String keywords = configuration.getString(Key.KEYWORDS, Content.KEYWORDS);
            this.configuration.set(Key.KEYWORDS, keywords);

            Integer batchSize = configuration.getInt(Key.BATCH_SIZE, Content.BATCHSIZE);
            this.configuration.set(Key.BATCH_SIZE, batchSize);
        }

        @Override public List<Configuration> split(int adviceNumber) {
            LOG.info("SolrReader Job start split.");
            int splitNum = this.configuration.getInt(Key.SPLIT_NUM, Content.SPLIT_NUM);
            List<Configuration> configurations = new ArrayList<>();
            long numTotal = SolrReaderUtils.queryTotal(this.configuration);
            // 单线程处理情况
            if (1 == splitNum) {
                Configuration tmpConf = this.configuration.clone();
                tmpConf.set(Key.START_PAGE, 1);
                tmpConf.set(Key.TOTAL, numTotal);
                configurations.add(tmpConf);
            } else {
                configurations = splitPage(configurations, splitNum, numTotal);
            }
            LOG.info("SolrReader Job end split.");
            return configurations;
        }

        private List<Configuration> splitPage(List<Configuration> configurations, int splitNum, long numTotal) {
            int batchSize = this.configuration.getInt(Key.BATCH_SIZE);
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
                this.configuration.set(Key.BATCH_SIZE, number);
            }
            // 分页间隔
            long pageInter = number / batchSize;
            Configuration tmpConf = null;
            for (int i = 0; i < splitNum; i++) {
                tmpConf = this.configuration.clone();
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

        @Override public void destroy() {

        }
    }

    public static class Task extends Reader.Task {

        private static Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private HttpSolrClient solrClient;
        private String keywords;
        private List<String> columns;
        private int batchSize;

        @Override
        public void init() {
            this.readerSliceConfig = this.getPluginJobConf();
            this.keywords = readerSliceConfig.getString(Key.KEYWORDS);
            this.columns = readerSliceConfig.getList(Key.COLUMN, String.class);
            this.batchSize = readerSliceConfig.getInt(Key.BATCH_SIZE);
            String solrUrl = readerSliceConfig.getString(Key.SOLR_URL);
            solrClient = new HttpSolrClient.Builder(solrUrl).build();
        }

        @Override
        public void startRead(RecordSender recordSender) {

            QueryResponse query;
            // 分页累加器
            int page = this.readerSliceConfig.getInt(Key.START_PAGE);
            Long total = this.readerSliceConfig.getLong(Key.TOTAL);
            LOG.info("线程-{},即将读取的数据总量为：{}.", Thread.currentThread().getName(), total);
            long status = 0;
            try {
                while (true) {
                    SolrQuery solrQuery = new SolrQuery();
                    // 查询字符串
                    solrQuery.set("q", keywords);
                    // 只查询指定域
                    solrQuery.set("fl", StringUtils.join(columns, ","));
                    // 分页
                    solrQuery.setStart((page - 1) * batchSize);
                    solrQuery.setRows(batchSize);
                    query = solrClient.query(solrQuery);
                    SolrDocumentList results = query.getResults();
                    status += results.size();
                    if (results.isEmpty() || status > total) {
                        break;
                    }
                    for (SolrDocument result : results) {
                        Record record = recordSender.createRecord();
                        for (String column : columns) {
                            Object obj = result.get(column);
                            if (obj instanceof List) {
                                String res = StringUtils.join((List<String>) obj, ",");
                                record.addColumn(new StringColumn(readerSliceConfig.trim(res)));
                            } else {
                                record.addColumn(
                                    new StringColumn(readerSliceConfig.trim(obj.toString())));
                            }
                        }
                        recordSender.sendToWriter(record);
                    }
                    page++;
                }
                solrClient.commit();
            } catch (Exception e) {
                LOG.error("reader solr data error, msg: {}", e.getMessage(), e);
                throw DataXException.asDataXException(
                    SolrReaderErrorCode.REQUIRED_VALUE, e.getMessage());
            }
        }

        @Override
        public void destroy() {
            if (null != solrClient) {
                try {
                    solrClient.close();
                } catch (IOException e) {
                    LOG.error("close solr client error. msg:{}", e.getMessage(), e);
                }
            }
        }
    }
}
