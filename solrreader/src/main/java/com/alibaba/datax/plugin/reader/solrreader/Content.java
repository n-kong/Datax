package com.alibaba.datax.plugin.reader.solrreader;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/5/20
 */
public class Content {

    /**
     * 匹配要查询哪些数据，默认全部
     */
    public static String KEYWORDS = "*:*";

    /**
     * 批处理大小，单次查询的数据量
     */
    public static int BATCHSIZE = 1000;

    /**
     * 读取线程数，默认为1，一个线程对应一个输出文件
     */
    public static int SPLIT_NUM = 1;
}
