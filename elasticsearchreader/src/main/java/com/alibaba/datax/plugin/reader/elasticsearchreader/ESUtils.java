package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.aliases.AddAliasMapping;
import io.searchbox.indices.aliases.AliasMapping;
import io.searchbox.indices.aliases.GetAliases;
import io.searchbox.indices.aliases.ModifyAliases;
import io.searchbox.indices.aliases.RemoveAliasMapping;
import io.searchbox.indices.mapping.PutMapping;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
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
public class ESUtils {


    private static final Logger log = LoggerFactory.getLogger(ESUtils.class);

    private JestClient jestClient;
    private Configuration conf;

    public ESUtils(Configuration conf) {
        this.conf = conf;
    }

    public JestClient getClient() {
        if (null == jestClient) {
            synchronized (ESUtils.class) {
                if (null == jestClient) {
                    jestClient = initClient();
                }
            }
        }
        return jestClient;
    }

    public JestClient initClient() {
        String endPoint = conf.getString(Key.ENDPOINT);
        String user = conf.getString(Key.USER);
        String password = conf.getString(Key.PASSWORD);

        // http请求，是否开启多线程，默认true
        Boolean multiThread = conf.getBool(Key.MULTI_THREAD, Content.MULTI_THREAD);
        // 超时
        int timeout = conf.getInt(Key.TIMEOUT, Content.TIMEOUT);
        // http开启压缩，默认true
        Boolean compression = conf.getBool(Key.COMPRESSION, Content.COMPRESSION);
        // 启用节点发现将轮询并定期更新客户机中的服务器列表，默认false
        Boolean discovery = conf.getBool(Key.DISCOVERY, Content.DISCOVERY);

        return createClient(endPoint, user, password, multiThread, timeout, compression, discovery);
    }

    public JestClient createClient(String endpoint,
        String user,
        String passwd,
        boolean multiThread,
        int readTimeout,
        boolean compression,
        boolean discovery) {

        JestClientFactory factory = new JestClientFactory();
        HttpClientConfig.Builder httpClientConfig = new HttpClientConfig
            .Builder(endpoint)
            .setPreemptiveAuth(new HttpHost(endpoint))
            .multiThreaded(multiThread)
            .connTimeout(60000)
            .readTimeout(readTimeout)
            .maxTotalConnection(200)
            .requestCompressionEnabled(compression)
            .discoveryEnabled(discovery)
            .discoveryFrequency(5l, TimeUnit.MINUTES);

        if (!("".equals(user) || "".equals(passwd))) {
            httpClientConfig.defaultCredentials(user, passwd);
        }

        factory.setHttpClientConfig(httpClientConfig.build());

        return factory.getObject();
    }

    public long queryTotal() {
        JestClient jestClient = getClient();
        String index = conf.getString(Key.INDEX);
        String type = conf.getString(Key.TYPE);
        // 构建搜索项
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        Map<String, Object> matchValue = conf.getMap(Key.MATCH_KEY, null, null);
        if (null != matchValue) {
            Set<Map.Entry<String, Object>> entries = matchValue.entrySet();
            Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> data = iterator.next();
                queryBuilder.must(QueryBuilders.matchQuery(data.getKey(), data.getValue()));
            }
        }
        searchSourceBuilder.query(queryBuilder);
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(index).addType(type).build();
        int value = 0;
        try {
            SearchResult result = jestClient.execute(search);
            value = result.getTotal();
        } catch (IOException e) {
            log.error("查询符合条件的数据总量失败，msg:{}", e.getMessage(), e);
        }
        return value;

    }


    /**
     * 关闭JestClient客户端
     *
     */
    public void closeJestClient() {
        if (null != jestClient) {
            jestClient.shutdownClient();
        }
    }


}
