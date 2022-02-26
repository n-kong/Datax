package com.alibaba.datax.plugin.reader.txtfilereader;

/**
 * Created by haiwei.luo on 14-9-20.
 */
public class Key {
	public static final String PATH = "path";
	public static final String MAX_FILE_NUM = "maxFileNum";

	public static final String BUCKET = "bucket";
	public static final String OBJECT = "object";
	public static final String MAX_FILE_SIZE = "maxFileSize";
	public static final String SUFFIX = "suffix";
	public static final String COMPRESS = "compress";

	/**
	 * 文件模式，table：表形式读取 oss：直接上传到oss ftp：直接上传到ftp
	 */
	public static final String FILE_MODEL = "fileModel";

	/**
	 * FTP
	 */
	public static final String PROTOCOL = "protocol";
	public static final String HOST = "host";
	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";
	public static final String PORT = "port";
	public static final String TIMEOUT = "timeout";
	public static final String TARGET_DIR = "targetDir";
}
