package com.alibaba.datax.plugin.reader.txtfilereader;

/**
 * Created by haiwei.luo on 14-9-20.
 */
public class Constant {
	public static final String SOURCE_FILES = "sourceFiles";
	public static final String IS_DELETE = "isDelete";
	public static final String FILE_PATH = "filePath";
	public static final String IS_OSS_FILE = "isOssFile";

	public static final Long MAX_FILE_SIZE = 1024 * 1024 * 10 * 10000L;

	/**
	 * 数据读取模式: 1单次 2数据驱动
	 */
	public static final String READ_MODE = "readMode";
	public static final String FILE_MODEL = "table";

	public static final int TIMEOUT = 60000;
}
