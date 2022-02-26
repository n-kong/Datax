package com.alibaba.datax.plugin.reader.ftpreader;

public class Key {
	public static final String PROTOCOL = "protocol";
	public static final String HOST = "host";
	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";
	public static final String PORT = "port";	
	public static final String TIMEOUT = "timeout";
	public static final String CONNECTPATTERN = "connectPattern";
	public static final String PATH = "path";
	public static final String MAXTRAVERSALLEVEL = "maxTraversalLevel";
	public static final String IS_IMAGE = "isImage";
	public static final String CHECK_FILE_PATH = "checkFilePath";

	public static final String FIELD_DELIMITER = "fieldDelimiter";
	public static final String DEAL_FILE = "dealFile";
	public static final String TARGET_Dir = "targetDir";

	/**
	 * ftpreader模式， table：目录下的数据当做二进制表来读， file：目录下的数据当做一个数据文件来读
	 */
	public static final String MODEL = "model";

	/**
	 * 输出的临时目录
	 */
	public static final String OUT_TMP_PATH = "outTmpPath";

	/**
	 * 输出的最终目录
	 */
	public static final String OUT_PATH = "outPath";

	public static final String CONST_COLUMN = "constColumn";
}
