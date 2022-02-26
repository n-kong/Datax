package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.List;

public class RedisReader extends Reader {

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory
            .getLogger(Job.class);

        private Configuration originalConfig = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            RedisProxy.checkNecessaryConfig(originalConfig);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.info("RedisReader Job start split.");
            List<Configuration> configurations = new ArrayList<Configuration>();
            configurations.add(this.originalConfig.clone());
            LOG.info("RedisReader Job end split.");
            return configurations;
        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;
        private Jedis jedis = null;
        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare() {
            Redis redis = new Redis(readerSliceConfig);
            jedis = redis.getJedis();
            jedis.select(readerSliceConfig.getInt(Key.DB_INDEX, 0));
        }

        @Override
        public void startRead(RecordSender recordSender) {

            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams paramas = new ScanParams();
            paramas.match(readerSliceConfig.getString(Key.MATCH_KEY));
            paramas.count(readerSliceConfig.getInt(Key.BATCH_SIZE, 100));
            RedisProxy redisProxy = new RedisProxy(recordSender, readerSliceConfig);
            while (true) {
                ScanResult<String> scanResult = jedis.scan(cursor, paramas);
                List<String> keys = scanResult.getResult();
                if (null != keys && !keys.isEmpty()) {
                    // write
                    for (String key : keys) {
                        String value = jedis.get(key);
                        redisProxy.doRead(value);
                    }
                }
                cursor = scanResult.getCursor();
                if ("0".equals(cursor)) {
                    break;
                }
            }
        }

        @Override
        public void destroy() {
            jedis.close();
        }
    }

}
