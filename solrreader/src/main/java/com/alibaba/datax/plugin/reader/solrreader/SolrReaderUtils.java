package com.alibaba.datax.plugin.reader.solrreader;

import com.alibaba.datax.common.util.Configuration;
import java.io.IOException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/5/24
 */
public class SolrReaderUtils {

    private static Logger LOG = LoggerFactory.getLogger(SolrReaderUtils.class);

    public static long queryTotal(Configuration configuration) {
        String url = configuration.getString(Key.SOLR_URL);
        long numTotal = 0;
        HttpSolrClient solrClient = null;
        try {
            solrClient = new HttpSolrClient.Builder(url).build();
            SolrQuery solrQuery = new SolrQuery();
            // 查询字符串
            solrQuery.set("q", configuration.getString(Key.KEYWORDS));
            QueryResponse resp = solrClient.query(solrQuery);
            numTotal = resp.getResults().getNumFound();
        } catch (Exception e) {
            LOG.error("查询SolrCore总数时失败。");
        } finally {
            try {
                if (null != solrClient) {
                    solrClient.close();
                }
            } catch (IOException e) {
                LOG.error("关闭SolrClient时失败。");
            }
        }
        return numTotal;
    }
}
