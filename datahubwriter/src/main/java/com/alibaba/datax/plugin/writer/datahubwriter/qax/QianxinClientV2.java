package com.alibaba.datax.plugin.writer.datahubwriter.qax;

import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code :
 * @since : Created in 14:47 2019/10/31
 */
public class QianxinClientV2 {

	private Logger logger = LoggerFactory.getLogger(QianxinClientV2.class);

	private MTokenCache mTokenCache = null;
	private String nameHost;

	public QianxinClientV2(String appId, String appKey, String tokenUrl, String nameHost) {
		mTokenCache = new MTokenCache(appId, appKey, tokenUrl);
		this.nameHost = nameHost;
	}

	public String sendPost(String url, String jsonString, Map <String, String> head) {
		return sendPost(url, this.nameHost, jsonString, head);
	}

	public String sendGet(String url, Map <String, String> head) {
		return sendGet(url, this.nameHost, head);
	}

	private String sendPost(String url, String host, String jsonString, Map <String, String> head) {
		if (jsonString == null) {
			jsonString = "{}";
		}
		if (head == null) {
			head = new HashMap <>();
		}
		//logger.debug("360奇安信客户端 - sendPost - bodyParam: " + jsonString);
		logger.debug("360奇安信客户端 - sendPost - headerParam: " + JSONObject.toJSONString(head));

		head.put("X-trustagw-access-token", mTokenCache.getToken());
		head.put("Host", host);

		String result = HttpUtils.httpsPost(url, head, jsonString);
		logger.debug("360奇安信客户端 - sendPost - response: {}", result);

		return result;
	}

	private String sendGet(String url, String host, Map <String, String> head) {
		if (head == null) {
			head = new HashMap <>();
		}
		logger.debug("360奇安信客户端 - sendGet - headerParam: " + JSONObject.toJSONString(head));

		head.put("X-trustagw-access-token", mTokenCache.getToken());
		head.put("Host", host);
		String result = HttpUtils.httpsGet(url, head);
		logger.debug("360奇安信客户端 - sendGet - response: {}", result);
		return result;
	}

	public static void main(String[] args) {

		Map <String, String> qaxHead = new HashMap <>();
		qaxHead.put("sender", "GabPersonInfosQuery");

	}
}
