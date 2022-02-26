package com.alibaba.datax.plugin.reader.redisreader;

/**
 * @author nkong
 * @version 1.0
 * @date 2021/1/10 21:52
 */
public class Content {

    // 可用连接实例的最大数目，默认值为20
    public static int MAX_TOTAL = 20;

    //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是10
    public static int MAX_IDLE = 10;

    //等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
    public static int MAX_WAIT = -1;

    public static int TIMEOUT = 60000;

    public static boolean TEST_ON_BORROW = false;
}
