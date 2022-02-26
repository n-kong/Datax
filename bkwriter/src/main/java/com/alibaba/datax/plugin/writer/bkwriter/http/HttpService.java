package com.alibaba.datax.plugin.writer.bkwriter.http;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * http
 */

public class HttpService {
    private static final String ENCODE = "UTF-8";
    private static Logger log = LoggerFactory.getLogger(HttpService.class);
    private static CloseableHttpClient httpclient = HttpClients.createDefault();
    /** 设置超时 */
    private static RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(180000)
        .setConnectTimeout(60000).setConnectionRequestTimeout(60000)
        .build();

    /**
     * 发送get请求,并返回响应 若请求发送失败，返回null
     */
    public static String sendGet(String url, Map<String, String> headers) {
        log.info("send http get：[{}]", url);
        String result = null;
        HttpResponse response = null;
        InputStream instream = null;
        HttpGet method = null;
        try {
            method = new HttpGet(url);
            //补充自定义信息头
            Iterator<Map.Entry<String, String>> headerIterator = headers.entrySet().iterator();
            while (headerIterator.hasNext()) {
                Map.Entry<String, String> entry = headerIterator.next();
                String key = entry.getKey();
                String value = entry.getValue();
                method.setHeader(key, value);
            }
            response = httpclient.execute(method);

            // 获取返回验证码
            int statusCode = response.getStatusLine().getStatusCode();
            // 返回不是成功状态码
            if (HttpStatus.SC_OK != statusCode) {
                return result;
            }
            HttpEntity entity = response.getEntity();
            if (null != entity) {
                instream = entity.getContent();
                result = IOUtils.toString(instream, ENCODE);
            }
            return result;
        } catch (Exception e) {
            log.error("http get request failed, url:{},{}", url, e);
            return result;
        } finally {
            try {
                IOUtils.closeQuietly(instream);
            } catch (Exception e) {
                log.error("closed connection failed {}", e);
            }
            if (null != response) {
                try {
                    EntityUtils.consume(response.getEntity());
                } catch (IOException e) {
                    log.error("close connection failed {}", e);
                }
            }
            try {
                if (null != method) {
                    method.releaseConnection();
                }
            } catch (Exception e) {
                log.error("close connection failed {}", e);
            }
        }
    }

    /**
     * 调用多媒体接口，返回结果数据
     *
     * @param url 请求地址
     * @return result 结果的Json字符串
     */
    public static String sendPost(String url, Map<String, String> headers, String entityStr) {
        //log.info("send http post：[{}]-[{}]",url,entityStr);
        String result = "";
        CloseableHttpResponse httpResponse = null;
        HttpPost httpPost = null;
        try {
            httpPost = new HttpPost(url);
            // 设置请求信息
            StringEntity entity = new StringEntity(entityStr, Charset.forName(ENCODE));
            httpPost.setConfig(requestConfig);
            //补充自定义信息头
            Iterator<Map.Entry<String, String>> headerIterator = headers.entrySet().iterator();
            while (headerIterator.hasNext()) {
                Map.Entry<String, String> entry = headerIterator.next();
                String key = entry.getKey();
                String value = entry.getValue();
                httpPost.setHeader(key, value);
            }
            httpPost.setHeader("Content-Charset", ENCODE);
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setHeader("Accept-Charset", ENCODE);
            httpPost.setEntity(entity);
            // 发送请求获取响应
            httpResponse = httpclient.execute(httpPost);
            // 判断获取响应的状态
            if (HttpStatus.SC_OK != httpResponse.getStatusLine().getStatusCode()) {
                log.error("http request failed, url:{}", url);
                return result;
            } else {
                // 获取返回的结果体
                HttpEntity resultEntity = httpResponse.getEntity();
                result = EntityUtils.toString(resultEntity, ENCODE);
                EntityUtils.consume(resultEntity);
                return result;
            }
        } catch (Exception e) {
            log.error("http request failed, url:{},{}", url, e);
            return result;
        } finally {
            if (httpResponse != null) {
                try {
                    httpResponse.close();
                } catch (IOException e) {
                    log.error("https request failed, url:{},{}", url, e);
                }
            }
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
        }
    }

}
