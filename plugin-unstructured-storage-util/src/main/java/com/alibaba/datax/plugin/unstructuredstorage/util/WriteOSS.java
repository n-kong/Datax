package com.alibaba.datax.plugin.unstructuredstorage.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.Key;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.ObjectMetadata;
import java.io.ByteArrayInputStream;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteOSS {
	private final static Logger LOGGER = LoggerFactory.getLogger(WriteOSS.class);
	private String oss_endPoint;
	private String accessKeyId;
	private String accessKeySecret;
	private String bucketName;
	private Configuration config;
	private OSSClient ossClient;

	public WriteOSS(Configuration config) {
		this.config = config;
		this.bucketName = config.getString(Key.BUCKET_NAME);
		initOssClient();
	}

	private void initOssClient() {
		this.oss_endPoint = config.getString(Key.OSS_ENDPOINT);
		this.accessKeyId = config.getString(Key.ACCESS_ID);
		this.accessKeySecret = config.getString(Key.ACCESS_KEY);
		this.ossClient = new OSSClient(oss_endPoint, accessKeyId, accessKeySecret);
	}

	public void writeOss(String key, byte[] bytes) {
		try {
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentType("image/jpeg");
			ossClient.putObject(bucketName, key, new ByteArrayInputStream(bytes),metadata);
			//判断文件是否存在
			boolean found = ossClient.doesObjectExist(bucketName, key);
			if (found) {
				LOGGER.info("key:[{}] write to oss success!", key);
			} else {
				LOGGER.info("key:[{}] write to oss failed!", key);
			}
		} catch (OSSException oe) {
			LOGGER.error("Caught an OSSException, which means your request made it to OSS, "
					+ "but was rejected with an error response for some reason.");
			LOGGER.error("Error Message: " + oe.getErrorCode());
			LOGGER.error("Error Code:       " + oe.getErrorCode());
			LOGGER.error("Request ID:      " + oe.getRequestId());
			LOGGER.error("Host ID:           " + oe.getHostId());
		} catch (ClientException ce) {
			LOGGER.error("Caught an ClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with OSS, "
					+ "such as not being able to access the network.");
			LOGGER.error("Error Message: " + ce.getMessage());
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}