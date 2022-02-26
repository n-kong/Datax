package com.alibaba.datax.plugin.writer.datahubwriter.qax;

import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code :
 * @since : Created in 18:59 2019/12/13
 */
public class HttpUtils {
	private static Logger logger = LoggerFactory.getLogger(HttpUtils.class);
	private static final String ENCODING = "UTF-8";

	private static RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(1000)
			.setSocketTimeout(1000).build();

	private static HttpClient httpClient = createSSLClientDefault();

	private HttpUtils() {
	}

	private static CloseableHttpClient createSSLClientDefault() {
		try {
			//使用 loadTrustMaterial() 方法实现一个信任策略，信任所有证书
			SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
				// 信任所有
				@Override
				public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
					return true;
				}
			}).build();
			//NoopHostnameVerifier类:  作为主机名验证工具，实质上关闭了主机名验证，它接受任何
			//有效的SSL会话并匹配到目标主机。
			HostnameVerifier hostnameVerifier = NoopHostnameVerifier.INSTANCE;
			SSLConnectionSocketFactory
				sslsf = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
			return HttpClients.custom().setSSLSocketFactory(sslsf).build();
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
			//logger.error("获取SSLClient遇到异常, 异常原因为:{}", e.getMessage());
		}
		return HttpClients.createDefault();
	}

	public static String httpsGet(String url, Map <String, String> headers) {
		String result = null;
		HttpGet get = new HttpGet(url);
		try {

			if (headers != null && !headers.isEmpty()) {
				for (Map.Entry <String, String> entry : headers.entrySet()) {
					get.setHeader(entry.getKey(), entry.getValue());
				}
			}
			HttpResponse response = httpClient.execute(get);
			// 检验http返回码
			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_CREATED) {
				result = EntityUtils.toString(response.getEntity(), ENCODING);
				//System.out.println("返回的结果是result");
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("发送post请求遇到问题。问题原因为{}", e.getMessage());
		} finally {
			get.releaseConnection();
		}
		return result;
	}

	public static String httpsPost(String url, Map <String, String> headers, String jsonStringParam) {
		String result = null;
		HttpPost post = new HttpPost(url);
		//post.setConfig(requestConfig);
		try {
			//https不验证证书
			//HttpClient httpClient = createSSLClientDefault();


			if (headers != null && !headers.isEmpty()) {
				for (Map.Entry <String, String> entry : headers.entrySet()) {
					post.setHeader(entry.getKey(), entry.getValue());
				}
			}
			// 构造消息头
			if (!headers.containsKey("Content-type")) {
				post.setHeader("Content-type", "application/json; charset=utf-8");
			}

			// 发送Json格式的数据请求
			StringEntity entity = new StringEntity(jsonStringParam, Charset.forName(ENCODING));
			entity.setContentEncoding(ENCODING);
			entity.setContentType("application/json");
			post.setEntity(entity);
			HttpResponse response = httpClient.execute(post);

			// 检验http返回码
			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_CREATED) {
				result = EntityUtils.toString(response.getEntity(), ENCODING);
				//System.out.println("返回的结果是result");
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("发送post请求遇到问题。问题原因为{}", e.getMessage());
		} finally {
			post.releaseConnection();
		}
		return result;
	}
}
