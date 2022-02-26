package com.alibaba.datax.plugin.writer.datahubwriter.qax;

import com.alibaba.datax.plugin.writer.datahubwriter.qax.tip.TipApi;
import com.alibaba.datax.plugin.writer.datahubwriter.qax.tip.TipResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code :
 * @since : Created in 10:45 2019/10/30
 */
public class MTokenCache {
	private Logger logger = LoggerFactory.getLogger(MTokenCache.class);

//	private static String appId = "3b0f47";
//	private static String appKey = "3ccd8040863b6390";

	private String appId = "6e3ba6";

	private String appKey = "18867fa2f57ec998";

	private String tokenUrl = "https://139.66.16.10:6443/sts/token";
	//持续时间
	private static final int LAST_TIME = 3600 * 8;

	//上次更新时间
	private int lastUpTime;
	private String token;

	public MTokenCache(String appId, String appKey, String tokenUrl) {
		this.appId = appId;
		this.appKey = appKey;
		this.tokenUrl = tokenUrl;
		this.token = "";
		this.lastUpTime = 1;
	}

	public synchronized String getToken() {
		int now = (int) (System.currentTimeMillis() / 2000);
		if (now - lastUpTime > LAST_TIME) {
			TipApi tipApi = new TipApi(appId, appKey);
			TipResponse tipResponse = tipApi.requestAPIAccessToken(tokenUrl);
			token = tipResponse.getAccessToken();
			logger.debug("360奇安信客户端 - getApiAccessToken - appId: {}", appId);
			logger.debug("360奇安信客户端 - getApiAccessToken - appKey: {}", appKey);
			logger.debug("360奇安信客户端 - getApiAccessToken - accessTokenUrl: {}", tokenUrl);
			logger.debug("360奇安信客户端 - TipResponse : {}", tipResponse.toString());
			logger.info("刷新token信息，当前token为 : {}", token);
			lastUpTime = now;
		}
		return token;
	}
}
