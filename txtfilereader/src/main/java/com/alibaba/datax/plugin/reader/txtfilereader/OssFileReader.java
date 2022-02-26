package com.alibaba.datax.plugin.reader.txtfilereader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.writer.osswriter.util.OssUtil;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/1/18
 */
public class OssFileReader {



    private static final Logger LOG = LoggerFactory.getLogger(OssFileReader.class);
    private OSSClient ossClient;
    private Configuration configuration;
    private String bucket;
    private String object;
    private String compress;
    private InitiateMultipartUploadRequest request = null;
    private InitiateMultipartUploadResult uploadResult = null;
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public OssFileReader(Configuration readerSliceConfig) {
        this.configuration = readerSliceConfig;
        this.init();
    }

    public void init() {
        this.ossClient = OssUtil.initOssClient(this.configuration);
        this.bucket = this.configuration.getString(Key.BUCKET);
        this.object = this.configuration.getString(Key.OBJECT);
        this.compress = this.configuration.getString(Key.COMPRESS);
    }

    // D:\workspace\DataX\aaa_file\read\20210115_TEST_1610720140113-00000.nb
    public void uploadFileToOss(List<String> files, RecordSender recordSender, Boolean isDelete) {
        for (String path : files) {
            File file = new File(path);
            InputStream inputStream = null;
            try {
                inputStream = new FileInputStream(file);
                if (null == compress) {
                    this.uploadPart(inputStream, file.getName(), recordSender);
                } else if ("zip".equalsIgnoreCase(compress)) {
                    ZipInputStream zipInputStream = new ZipInputStream(inputStream, Charset.forName("GBK"));
                    ZipFile zipFile = new ZipFile(file);
                    ZipEntry zipEntry = null;
                    while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                        inputStream = zipFile.getInputStream(zipEntry);
                        this.uploadPart(inputStream, zipEntry.getName(), recordSender);
                    }
                    zipInputStream.close();
                }
                if (isDelete) {
                    file.delete();
                }
            } catch (Exception e) {
                LOG.error("read file:[{}] error.", path, e);
            } finally {
                IOUtils.closeQuietly(inputStream);
            }
        }

    }

    private void uploadPart(final InputStream inputStream, String fileName, RecordSender recordSender) {
        final String currentObject = String.format("%s/%s", this.object, fileName);
        request = new InitiateMultipartUploadRequest(bucket, currentObject);
        // 在初始化分片时设置文件存储类型
        ObjectMetadata metadata = new ObjectMetadata();
        String contentType = getContentType(fileName);
        metadata.setContentType(contentType);
        request.setObjectMetadata(metadata);

        uploadResult = this.ossClient.initiateMultipartUpload(request);
        final String uploadId = uploadResult.getUploadId();
        // partETagsETags是PartETag的集合。PartETag由分片的ETag和分片号组成
        final List<PartETag> partETags = new ArrayList<PartETag>();
        // 计算文件有多少个分片  10MB
        final long partSize = 10 * 1024 * 1024L;
        try {
            int available = inputStream.available();
            long numberCal = available / partSize;
            long partCount = numberCal >= 1 ? numberCal : 1;
            partCount = partCount > 10000 ? 10000 : partCount;
            // 遍历分片上传。
            for (int i = 0; i < partCount; i++) {
                long startPos = i * partSize;
                final long curPartSize = (i + 1 == partCount) ? (available - startPos) : partSize;
                // 跳过已经上传的分片。
                inputStream.skip(startPos);
                final int partNumber = i + 1;
                RetryUtil.executeWithRetry(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        // 创建UploadPartRequest，上传分块
                        UploadPartRequest uploadPartRequest = new UploadPartRequest();
                        uploadPartRequest.setBucketName(bucket);
                        uploadPartRequest.setKey(currentObject);
                        uploadPartRequest.setUploadId(uploadId);
                        uploadPartRequest.setInputStream(inputStream);
                        // 设置分片大小。除了最后一个分片没有大小限制，其他的分片最小为100 KB。
                        uploadPartRequest.setPartSize(curPartSize);
                        // 设置分片号。每一个上传的分片都有一个分片号，取值范围是1~10000，如果超出这个范围，OSS将返回InvalidArgument的错误码。
                        uploadPartRequest.setPartNumber(partNumber);
                        // 每个分片不需要按顺序上传，甚至可以在不同客户端上传，OSS会按照分片号排序组成完整的文件。
                        UploadPartResult uploadPartResult = ossClient.uploadPart(uploadPartRequest);
                        // 每次上传分片之后，OSS的返回结果包含PartETag。PartETag将被保存在partETags中。
                        partETags.add(uploadPartResult.getPartETag());
                        return true;
                    }
                }, 3, 1000L, false);

            }

        } catch (Exception e) {
            LOG.error("upload file[{}] to oss file.", fileName, e);
        }
        // 创建CompleteMultipartUploadRequest对象。
        // 在执行完成分片上传操作时，需要提供所有有效的partETags。
        // OSS收到提交的partETags后，会逐一验证每个分片的有效性。
        // 当所有的数据分片验证通过后，OSS将把这些分片组合成一个完整的文件。
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
            new CompleteMultipartUploadRequest(bucket, currentObject, uploadId, partETags);

        // 如果需要在完成文件上传的同时设置文件访问权限，请参考以下示例代码。
        // completeMultipartUploadRequest.setObjectACL(CannedAccessControlList.PublicRead);

        // 完成上传。
        CompleteMultipartUploadResult completeMultipartUploadResult = ossClient.completeMultipartUpload(completeMultipartUploadRequest);
        LOG.info("final object etag is:[{}]", completeMultipartUploadResult.getETag());

        // write channel
        Record record = recordSender.createRecord();
        record.addColumn(new StringColumn(fileName));
        record.addColumn(new StringColumn(currentObject));
        record.addColumn(new StringColumn(sdf.format(new Date())));
        recordSender.sendToWriter(record);

    }


    public void close() {
        this.ossClient.shutdown();
    }

    public String getContentType(String fileName) {
        String type = fileName.substring(fileName.lastIndexOf(".") + 1);
        switch (type) {
            case "bmp":
                return "image/bmp";
            case "gif":
                return "image/gif";
            case "jpeg":
            case "jpg":
                return "image/jpeg";
            case "html":
                return "text/html";
            case "txt":
            case "nb":
            case "bcp":
                return "text/plain";
            case "vsd":
                return "application/vnd.visio";
            case "pptx":
            case "ppt":
                return "application/vnd.ms-powerpoint";
            case "docx":
            case "doc":
                return "application/msword";
            case "pdf":
                return "application/pdf";
            case "xml":
                return "text/xml";
            case "mp3":
                return "audio/mp3";
            case "mp4":
                return "video/mpeg4";
            case "wav":
                return "audio/wav";
            default:
                return "";
        }
    }

}
