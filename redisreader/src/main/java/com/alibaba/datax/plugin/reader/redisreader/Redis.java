package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.*;

import java.util.ArrayList;

/**
 * @author nkong
 * @version 1.0
 * @date 2021/1/10 21:46
 */
public class Redis {

    /**
     * 非切片连接池
     */
    private JedisPool jedisPool;

    /**
     * 切片连接池
     */
    private ShardedJedisPool shardedJedisPool;
    private String host = null;
    private int port = 6379;
    private String auth = null;
    private Configuration readerSliceConfig;

    public Redis(Configuration readerSliceConfig) {
        this.readerSliceConfig = readerSliceConfig;
        this.host = readerSliceConfig.getString(Key.HOST);
        this.port = readerSliceConfig.getInt(Key.PORT);
        this.auth = readerSliceConfig.getString(Key.AUTH);
    }

    /**
     * 初始化非切片连接池
     */
    public JedisPool getJedisPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(readerSliceConfig.getInt(Key.MAX_TOTAL, Content.MAX_TOTAL));
        config.setMaxIdle(readerSliceConfig.getInt(Key.MAX_IDLE, Content.MAX_IDLE));
        config.setMaxWaitMillis(readerSliceConfig.getInt(Key.MAX_WAIT, Content.MAX_WAIT));
        config.setTestOnBorrow(readerSliceConfig.getBool(Key.TEST_ON_BORROW, Content.TEST_ON_BORROW));
        jedisPool = new JedisPool(config, host, port, readerSliceConfig.getInt(Key.TIMEOUT, Content.TIMEOUT));
        return jedisPool;
    }

    /**
     * 初始化切片连接池
     */
    public ShardedJedisPool getShardedJedisPool() {
        // 池基本配置
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(Content.MAX_TOTAL);
        config.setMaxIdle(Content.MAX_IDLE);
        config.setMaxWaitMillis(Content.MAX_WAIT);
        config.setTestOnBorrow(Content.TEST_ON_BORROW);

        ArrayList<JedisShardInfo> list = new ArrayList<JedisShardInfo>();
        list.add(new JedisShardInfo(host, port, "master"));

        shardedJedisPool = new ShardedJedisPool(config, list);
        return shardedJedisPool;
    }

    public JedisPool getSingleJedisPool() {
        if (jedisPool == null) {
            synchronized (Redis.class) {
                if (jedisPool == null) {
                    return getJedisPool();
                }
            }
        }
        return jedisPool;
    }

    public Jedis getJedis() {
        Jedis jedis = getSingleJedisPool().getResource();
        if (!StringUtils.isBlank(auth)) {
            jedis.auth(auth);
        }
        return jedis;
    }

    public ShardedJedis getShardedJedis() {
        return shardedJedisPool.getResource();
    }

    /**
     * 释放
     *
     * @param jedis
     */
    public void free(final Jedis jedis) {
        jedis.close();
    }


}
