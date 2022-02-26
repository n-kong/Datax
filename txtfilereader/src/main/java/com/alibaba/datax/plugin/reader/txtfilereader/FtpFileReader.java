package com.alibaba.datax.plugin.reader.txtfilereader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.writer.ftpwriter.FtpWriterErrorCode;
import com.alibaba.datax.plugin.writer.ftpwriter.util.IFtpHelper;
import com.alibaba.datax.plugin.writer.ftpwriter.util.SftpHelperImpl;
import com.alibaba.datax.plugin.writer.ftpwriter.util.StandardFtpHelperImpl;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/6/18
 */
public class FtpFileReader {

    private static final Logger LOG = LoggerFactory.getLogger(FtpFileReader.class);

    private Configuration conf;
    private IFtpHelper ftpHelper = null;

    private String protocol;
    private String host;
    private int port;
    private String username;
    private String password;
    private int timeout;
    private String targetDir;

    public FtpFileReader(Configuration conf) {
        this.conf = conf;
        this.validateParameter();
        this.init();
    }

    private void validateParameter() {
        this.host = this.conf.getNecessaryValue(Key.HOST,
            FtpWriterErrorCode.REQUIRED_VALUE);
        this.username = this.conf.getNecessaryValue(Key.USERNAME, FtpWriterErrorCode.REQUIRED_VALUE);
        this.password = this.conf.getNecessaryValue(Key.PASSWORD, FtpWriterErrorCode.REQUIRED_VALUE);
        this.timeout = this.conf.getInt(Key.TIMEOUT, Constant.TIMEOUT);

        this.targetDir = conf.getString(Key.TARGET_DIR);

        this.protocol = conf.getString(Key.PROTOCOL, "ftp");
    }

    private void init() {

        if ("sftp".equalsIgnoreCase(this.protocol)) {
            this.port = this.conf.getInt(Key.PORT,
                com.alibaba.datax.plugin.writer.ftpwriter.util.Constant.DEFAULT_SFTP_PORT);
            this.ftpHelper = new SftpHelperImpl();
        } else if ("ftp".equalsIgnoreCase(this.protocol)) {
            this.port = this.conf.getInt(Key.PORT,
                com.alibaba.datax.plugin.writer.ftpwriter.util.Constant.DEFAULT_FTP_PORT);
            this.ftpHelper = new StandardFtpHelperImpl();
        }

        try {
            RetryUtil.executeWithRetry(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    ftpHelper.loginFtpServer(host, username, password,
                        port, timeout);
                    return null;
                }
            }, 3, 3000, true);
        } catch (Exception e) {
            String message = String
                .format("与ftp服务器建立连接失败, host:%s, username:%s, port:%s, errorMessage:%s",
                    host, username, port, e.getMessage());
            LOG.error(message);
            throw DataXException.asDataXException(
                FtpWriterErrorCode.FAIL_LOGIN, message, e);
        }

        this.ftpHelper.mkDirRecursive(targetDir);

    }

    public void uploadFileToFtp(List<String> sourceFiles, Boolean isDelete) {

        this.ftpHelper.uploadFile(sourceFiles, targetDir, isDelete);

    }
}
