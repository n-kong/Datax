package com.alibaba.datax.plugin.writer.datahubwriter.qax.tip;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
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
 * @author 360提供的获取tip_access_token的工具类
 */
public class TipApi {

	private Logger logger = LoggerFactory.getLogger(TipApi.class);

	private FingerTookit fingerTookit;
	private String appId;
	private String appKey;

	private static final String CHARSET = "UTF-8";



	public TipApi(String appId, String appKey) {
		this.appId = appId;
		this.appKey = appKey;
		fingerTookit = new FingerTookit(appId, appKey);
	}

	/**
	 * 获取API访问令牌
	 *
	 * @param url 申请token的url
	 * @return Map<String, Object> 返回信息
	 */
	public TipResponse requestAPIAccessToken(String url) throws JSONException {
		//填充消息
		JSONObject jobj = new JSONObject();
		jobj.put("app_id", this.appId);
		String fingerprint = fingerTookit.buildFingerprint(jobj);
		jobj.put("fingerprint", fingerprint);


		HttpPost post = null;
		try {
			//https不验证证书
			HttpClient httpClient = createSSLClientDefault();
			post = new HttpPost(url);

			// 构造消息头
			post.setHeader("Content-type", "application/json; charset=utf-8");

			// 构建消息实体
			// 发送Json格式的数据请求
			StringEntity entity = new StringEntity(jobj.toJSONString(), Charset.forName(CHARSET));
			entity.setContentEncoding(CHARSET);
			entity.setContentType("application/json");
			post.setEntity(entity);


			HttpResponse response = httpClient.execute(post);

			// 检验http返回码
			int statusCode = response.getStatusLine().getStatusCode();
			logger.info("httpResponse result is " + response);
			logger.info("httpResponse HttpStatus is " + statusCode);
			if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_CREATED) {
				String result = null;
				result = EntityUtils.toString(response.getEntity(), CHARSET);
				logger.info("httpResponse result is " + result);
				TipResponse tipResponse = new TipResponse();

				tipResponse.setAccessToken(JSONObject.parseObject(result).getString("access_token"));
				tipResponse.setExpiresIn(JSONObject.parseObject(result).getInteger("expires_in"));
				return tipResponse;
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("获取360Token信息遇到异常");
		} finally {
			if (post != null) {
				post.releaseConnection();
			}
		}
		return null;
	}


	public static CloseableHttpClient createSSLClientDefault() {
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
}
