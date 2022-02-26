package com.alibaba.datax.plugin.writer.bkwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/1/29
 */
public class BkWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();

        private Configuration writerSliceConfig = null;

        @Override
        public void init() {
            LOG.info("BkWriter job start init.");
            this.writerSliceConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare() {
            String imageType = writerSliceConfig.getString(Key.IMAGETYPE, "imageUrl");
            if (Constant.imageUrl.equalsIgnoreCase(imageType)) {
                imageType = "imageUrl";
            } else if (Constant.imageBase64.equalsIgnoreCase(imageType)) {
                imageType = "content";
            } else {
                throw DataXException.asDataXException(BkWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                    "亲，必填参数[imageType]填写错误，如果您要上传的图片类型为图片地址，请您填写imageUrl，如果您要上传的图片类型为图片base64编写，请您填写imageBase64。目前仅支持这两种类型。");
            }
            writerSliceConfig.set(Key.IMAGETYPE, imageType);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("BkWriter job start split.");
            List<Configuration> configurations = new ArrayList<Configuration>();

            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration tempConfig = this.writerSliceConfig.clone();
                configurations.add(tempConfig);
            }

            if (IS_DEBUG) {
                LOG.debug("After master split, the job config now is:[{}].",
                    this.writerSliceConfig);
            }

            return configurations;
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;
        private String url;
        private int objectIdIndex;
        private int imageIndex;
        private String bkId;
        private int batchSize;
        private String imageType;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.url = writerSliceConfig.getString(Key.URL);
            this.objectIdIndex = writerSliceConfig.getInt(Key.OBJECTIDINDEX);
            this.imageIndex = writerSliceConfig.getInt(Key.IMAGEINDEX);
            this.bkId = writerSliceConfig.getString(Key.BKID);
            this.batchSize = writerSliceConfig.getInt(Key.BATCHSIZE);
            this.imageType = writerSliceConfig.getString(Key.IMAGETYPE);
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {

            try {
                BkWriterProxy proxy =
                    new BkWriterProxy(url, objectIdIndex, imageIndex, bkId, batchSize, imageType);
                Record record = null;
                while ((record = lineReceiver.getFromReader()) != null) {
                    proxy.writeRecord(record);
                }
                proxy.writeRemainingRecord();
            } catch (Exception e) {
                throw DataXException.asDataXException(BkWriterErrorCode.Write_ERROR, "数据写入布控库失败，请您重新操作。");
            }

        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }
}