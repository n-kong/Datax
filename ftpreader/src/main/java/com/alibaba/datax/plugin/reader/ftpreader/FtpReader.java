package com.alibaba.datax.plugin.reader.ftpreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FtpReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;

        private List<String> path = null;

        private HashSet<String> sourceFiles;

        // ftp链接参数
        private String protocol;
        private String host;
        private int port;
        private String username;
        private String password;
        private int timeout;
        private String connectPattern;
        private int maxTraversalLevel;

        private FtpHelper ftpHelper = null;
        private String model;

        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();
            this.sourceFiles = new HashSet<String>();

            this.validateParameter();
            UnstructuredStorageReaderUtil.validateParameter(this.originConfig);

            if ("sftp".equals(protocol)) {
                //sftp协议
                this.port = originConfig.getInt(Key.PORT, Constant.DEFAULT_SFTP_PORT);
                this.ftpHelper = new SftpHelper();
            } else if ("ftp".equals(protocol)) {
                // ftp 协议
                this.port = originConfig.getInt(Key.PORT, Constant.DEFAULT_FTP_PORT);
                this.ftpHelper = new StandardFtpHelper();
            }
            ftpHelper.loginFtpServer(host, username, password, port, timeout, connectPattern);
        }

        private String checkPath(String path) {
            return path.endsWith("/") || path.endsWith("\\") ? path : path +
                IOUtils.DIR_SEPARATOR_UNIX;
        }

        private void validateParameter() {
            //todo 常量
            this.protocol = this.originConfig
                .getNecessaryValue(Key.PROTOCOL, FtpReaderErrorCode.REQUIRED_VALUE);
            boolean ptrotocolTag = "ftp".equals(this.protocol) || "sftp".equals(this.protocol);
            if (!ptrotocolTag) {
                throw DataXException.asDataXException(FtpReaderErrorCode.ILLEGAL_VALUE,
                    String.format("仅支持 ftp和sftp 传输协议 , 不支持您配置的传输协议: [%s]", protocol));
            }
            this.model = this.originConfig.getString(Key.MODEL, Constant.MODEL);
            boolean isPass = "table".equals(this.model) || "file".equals(this.model);
            if (!isPass) {
                throw DataXException.asDataXException(FtpReaderErrorCode.ILLEGAL_VALUE,
                    String.format("仅支持 table和file 模式的数据采集，不支持您配置的采集模式：[%s]", this.model));
            } else {
                this.originConfig.set(Key.MODEL, this.model);
                if ("file".equals(this.model)) {
                    String outTmpPath = this.originConfig
                        .getNecessaryValue(Key.OUT_TMP_PATH, FtpReaderErrorCode.REQUIRED_VALUE);
                    this.originConfig.set(Key.OUT_TMP_PATH, checkPath(outTmpPath));
                    String outPath = this.originConfig
                        .getNecessaryValue(Key.OUT_PATH, FtpReaderErrorCode.REQUIRED_VALUE);
                    this.originConfig.set(Key.OUT_PATH, checkPath(outPath));
                }
            }
            if ("move".equals(this.originConfig.getString(Key.DEAL_FILE))) {
                String tarPath = this.originConfig
                    .getNecessaryValue(Key.TARGET_Dir, FtpReaderErrorCode.REQUIRED_VALUE);
                this.originConfig.set(Key.TARGET_Dir, checkPath(tarPath));
            }

            this.host =
                this.originConfig.getNecessaryValue(Key.HOST, FtpReaderErrorCode.REQUIRED_VALUE);
            this.username = this.originConfig
                .getNecessaryValue(Key.USERNAME, FtpReaderErrorCode.REQUIRED_VALUE);
            this.password = this.originConfig
                .getNecessaryValue(Key.PASSWORD, FtpReaderErrorCode.REQUIRED_VALUE);
            this.timeout = originConfig.getInt(Key.TIMEOUT, Constant.DEFAULT_TIMEOUT);
            this.maxTraversalLevel =
                originConfig.getInt(Key.MAXTRAVERSALLEVEL, Constant.DEFAULT_MAX_TRAVERSAL_LEVEL);

            // only support connect pattern
            this.connectPattern = this.originConfig
                .getUnnecessaryValue(Key.CONNECTPATTERN, Constant.DEFAULT_FTP_CONNECT_PATTERN,
                    null);
            boolean connectPatternTag =
                "PORT".equals(connectPattern) || "PASV".equals(connectPattern);
            if (!connectPatternTag) {
                throw DataXException.asDataXException(FtpReaderErrorCode.ILLEGAL_VALUE,
                    String.format("不支持您配置的ftp传输模式: [%s]", connectPattern));
            } else {
                this.originConfig.set(Key.CONNECTPATTERN, connectPattern);
            }

            //path check
            String pathInString =
                this.originConfig.getNecessaryValue(Key.PATH, FtpReaderErrorCode.REQUIRED_VALUE);
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = new ArrayList<String>();
                path.add(pathInString);
            } else {
                path = this.originConfig.getList(Key.PATH, String.class);
                if (null == path || path.size() == 0) {
                    throw DataXException
                        .asDataXException(FtpReaderErrorCode.REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
                }
                for (String eachPath : path) {
                    if (!eachPath.startsWith("/")) {
                        String message = String.format("请检查参数path:[%s],需要配置为绝对路径", eachPath);
                        LOG.error(message);
                        throw DataXException
                            .asDataXException(FtpReaderErrorCode.ILLEGAL_VALUE, message);
                    }
                }
            }
        }

        @Override
        public void prepare() {
            LOG.debug("prepare() begin...");
            this.sourceFiles = ftpHelper.getAllFiles(path, 0, maxTraversalLevel);
            this.originConfig.set(Constant.SOURCE_FILES, this.sourceFiles);
            for (String sourceFile : this.sourceFiles) {
                LOG.info("即将读取的文件为：{}", sourceFile);
            }
            LOG.info(String.format("文件总数: [%s]个", this.sourceFiles.size()));
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            try {
                this.ftpHelper.logoutFtpServer();
            } catch (Exception e) {
                String message = String.format(
                    "关闭与ftp服务器连接失败: [%s] host=%s, username=%s, port=%s",
                    e.getMessage(), host, username, port);
                LOG.error(message, e);
            }
        }

        // warn: 如果源目录为空会报错，拖空目录意图=>空文件显示指定此意图
        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

            // warn:每个slice拖且仅拖一个文件,
            // int splitNumber = adviceNumber;
            int splitNumber = this.sourceFiles.size();
            if (originConfig.getBool(Key.IS_IMAGE, false)) {
                splitNumber = 1;
            }
            if (0 == splitNumber) {
                throw DataXException.asDataXException(FtpReaderErrorCode.EMPTY_DIR_EXCEPTION,
                    String.format("未能找到待读取的文件,请确认您的配置项path: %s",
                        this.originConfig.getString(Key.PATH)));
            }

            List<List<String>> splitedSourceFiles =
                this.splitSourceFiles(new ArrayList(this.sourceFiles), splitNumber);
            for (List<String> files : splitedSourceFiles) {
                Configuration splitedConfig = this.originConfig.clone();
                splitedConfig.set(Constant.SOURCE_FILES, files);
                readerSplitConfigs.add(splitedConfig);
            }
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }

        private <T> List<List<T>> splitSourceFiles(final List<T> sourceList, int adviceNumber) {
            List<List<T>> splitedList = new ArrayList<List<T>>();
            int averageLength = sourceList.size() / adviceNumber;
            averageLength = averageLength == 0 ? 1 : averageLength;

            for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
                end = begin + averageLength;
                if (end > sourceList.size()) {
                    end = sourceList.size();
                }
                splitedList.add(sourceList.subList(begin, end));
            }
            return splitedList;
        }
    }

    public static class Task extends Reader.Task {
        private static Logger LOG = LoggerFactory.getLogger(Task.class);

        private String host;
        private int port;
        private String username;
        private String password;
        private String protocol;
        private int timeout;
        private String connectPattern;

        private Configuration readerSliceConfig;
        private List<String> sourceFiles;

        private List<String> checkFiles = new ArrayList<String>();

        private FtpHelper ftpHelper = null;
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        private String path;
        private BufferedWriter writer = null;

        private String model;

        private List<String> constColumnList = new ArrayList<String>();
        private String outTmpPath;
        private String outPath;
        private String dealFile;
        private String fieldDelimiter;

        @Override
        public void init() {//连接重试
            /* for ftp connection */
            this.readerSliceConfig = this.getPluginJobConf();
            this.host = readerSliceConfig.getString(Key.HOST);
            this.protocol = readerSliceConfig.getString(Key.PROTOCOL);
            this.username = readerSliceConfig.getString(Key.USERNAME);
            this.password = readerSliceConfig.getString(Key.PASSWORD);
            this.timeout = readerSliceConfig.getInt(Key.TIMEOUT, Constant.DEFAULT_TIMEOUT);
            this.model = readerSliceConfig.getString(Key.MODEL);

            this.dealFile = readerSliceConfig.getString(Key.DEAL_FILE, null);
            this.fieldDelimiter = readerSliceConfig.getString(Key.FIELD_DELIMITER, "\t");

            String constCol = this.readerSliceConfig.getString(Key.CONST_COLUMN, null);
            if (StringUtils.isNotBlank(constCol)) {
                if (!constCol.startsWith("[") && !constCol.endsWith("]")) {
                    constColumnList.add(constCol);
                } else {
                    constColumnList =
                        this.readerSliceConfig.getList(Key.CONST_COLUMN, String.class);
                }
            }

            if ("file".equals(this.model)) {
                outTmpPath = this.readerSliceConfig.getString(Key.OUT_TMP_PATH);
                outPath = this.readerSliceConfig.getString(Key.OUT_PATH);
            }
            this.sourceFiles = this.readerSliceConfig.getList(Constant.SOURCE_FILES, String.class);

            if ("sftp".equals(protocol)) {
                //sftp协议
                this.port = readerSliceConfig.getInt(Key.PORT, Constant.DEFAULT_SFTP_PORT);
                this.ftpHelper = new SftpHelper();
            } else if ("ftp".equals(protocol)) {
                // ftp 协议
                this.port = readerSliceConfig.getInt(Key.PORT, Constant.DEFAULT_FTP_PORT);
                this.connectPattern = readerSliceConfig
                    .getString(Key.CONNECTPATTERN, Constant.DEFAULT_FTP_CONNECT_PATTERN);// 默认为被动模式
                this.ftpHelper = new StandardFtpHelper();
            }
            ftpHelper.loginFtpServer(host, username, password, port, timeout, connectPattern);

            if (readerSliceConfig.getBool(Key.IS_IMAGE, false)) {
                loadResource(readerSliceConfig.getString(Key.CHECK_FILE_PATH));
                try {
                    writer = new BufferedWriter(new FileWriter(new File(path), true));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private void loadResource(String filePath) {

            File file = new File(filePath);
            path = filePath;
            if (file.isDirectory()) {
                path = filePath + "/" + sdf.format(new Date()) + ".chk";
                file = new File(path);
            }
            BufferedReader reader = null;
            try {
                if (!file.exists()) {
                    if (file.getParentFile() != null && !file.getParentFile().exists()) {
                        file.getParentFile().mkdirs();
                    }
                    file.createNewFile();
                    return;
                }
                reader = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = reader.readLine()) != null) {
                    checkFiles.add(line);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            } finally {
                try {
                    if (null != reader) {
                        reader.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void prepare() {

        }

        @Override
        public void post() {

            if (null != this.dealFile) {
                for (String fileName : this.sourceFiles) {
                    if ("delete".equals(dealFile)) {
                        ftpHelper.deleteFile(fileName);
                    } else if ("move".equals(dealFile)) {
                        String targetDir = this.readerSliceConfig.getString(Key.TARGET_Dir);
                        String targetFile = targetDir + new File(fileName).getName();
                        LOG.info("targetFile is {}", targetFile);
                        ftpHelper.renameFile(fileName, targetFile);
                    } else {
                        LOG.warn("您配置的dealFile:[{}]参数不符合要求，本次执行不会删除或移动源文件，如需操作，请您配置为delete或move。", dealFile);
                    }
                }
            }
        }

        @Override
        public void destroy() {
            try {
                this.ftpHelper.logoutFtpServer();
            } catch (Exception e) {
                String message = String.format(
                    "关闭与ftp服务器连接失败: [%s] host=%s, username=%s, port=%s",
                    e.getMessage(), host, username, port);
                LOG.error(message, e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.debug("start read source files...");
            InputStream inputStream = null;
            try {
                // 以表的形式读取数据
                if ("table".equals(this.model)) {
                    for (String fileName : this.sourceFiles) {
                        fileName = fileName.replace("\\", "/");
                        // 读取ftp上的文本文件
                        if (!this.readerSliceConfig.getBool(Key.IS_IMAGE, false)) {
                            LOG.info(String.format("reading file : [%s]", fileName));
                            inputStream = ftpHelper.getInputStream(fileName);
                            // 判断是否需求追加输出流
                            if (!constColumnList.isEmpty()) {
                                inputStream = addStream(inputStream);
                            }
                            UnstructuredStorageReaderUtil
                                .readFromStream(inputStream, fileName, this.readerSliceConfig,
                                    recordSender, this.getTaskPluginCollector());
                            recordSender.flush();
                            // 读取ftp上的图片
                        } else {
                            //String key = fileName.substring(fileName.lastIndexOf("/") + 1);
                            String key = fileName;
                            if (checkFiles.contains(key)) {
                                continue;
                            }
                            LOG.info(String.format("reading file : [%s]", fileName));
                            String imageBase64 = ftpHelper.getFtpImage(fileName);
                            dealImageRecord(recordSender, imageBase64, key);
                            writeCheck(writer, key);
                        }
                    }
                    // 以文本文件的方式读取数据
                } else {
                    ftpHelper.downFile(this.sourceFiles, outTmpPath, outPath);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            } finally {
                IOUtils.closeQuietly(inputStream);
                IOUtils.closeQuietly(writer);
            }

            LOG.debug("end read source files...");
        }

        private InputStream addStream(InputStream inputStream) throws IOException {

            for (String s : constColumnList) {
                //切分常量列
                String[] split = s.split("::");
                //如果是STRING类型,则直接添加
                if ("STRING".equalsIgnoreCase(split[0])) {
                    inputStream = appendConstCol(inputStream, split[1]);
                } else if ("DATE".equals(split[0])) {
                    String dataStr = "";
                    String type = split[1];
                    int interval = Integer.parseInt(split[2]);
                    String format = split[3];
                    if ("YEAR".equalsIgnoreCase(type)) {
                        //Calendar.YEAR 对应的值是1
                        dataStr = getDate(1, interval, format);
                    } else if ("MONTH".equalsIgnoreCase(type)) {
                        //Calendar.MONTH对应的是2
                        dataStr = getDate(2, interval, format);
                    } else if ("DAY".equalsIgnoreCase(type)) {
                        //Calendar.DAY_OF_YEAR对应的是6
                        dataStr = getDate(6, interval, format);
                    } else if ("HOUR".equalsIgnoreCase(type)) {
                        //HOUR_OF_DAY对应的是11
                        dataStr = getDate(11, interval, format);
                    } else if ("MINUTE".equalsIgnoreCase(type)) {
                        //Calendar.MINUTE对应的是12
                        dataStr = getDate(12, interval, format);
                    } else {
                        throw DataXException.asDataXException(FtpReaderErrorCode.ERROR_TIME_TYPE,
                            "时间类型无效，应为，YEAR、MONTH、DAY、HOUR、MINUTE中的一种");
                    }
                    inputStream = appendConstCol(inputStream, dataStr);
                }
            }

            return inputStream;
        }

        private InputStream appendConstCol(InputStream inputStream, String constCol)
            throws IOException {

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            StringBuffer sb = new StringBuffer();
            String str = null;
            while ((str = bufferedReader.readLine()) != null) {
                sb.append(str).append(fieldDelimiter).append(constCol).append("\r\n");
            }
            String s = sb.toString();
            inputStream = IOUtils.toInputStream(s);
            //IOUtils.closeQuietly(bufferedReader);
            return inputStream;
        }

        private String getDate(int tyep, int interval, String format) {
            String data = "";
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            Calendar calendar = Calendar.getInstance();
            calendar.add(tyep, interval);
            Date date = calendar.getTime();
            data = sdf.format(date);
            return data;
        }

        /**
         * send zp to channel
         */
        private void dealImageRecord(RecordSender recordSender, String base64, String key) {
            Record record = recordSender.createRecord();
            record.addColumn(new StringColumn(key));
            record.addColumn(new StringColumn(base64));
            recordSender.sendToWriter(record);
        }

        /**
         * save checkpoint message
         */
        private void writeCheck(BufferedWriter writer, String key) {
            try {
                writer.write(key);
                writer.write("\r\n");
                writer.flush();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }
}
