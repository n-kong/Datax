package com.alibaba.datax.plugin.writer.txtfilewriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public class TxtFileWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;

        private static String test  = "kqlu";

        @Override
        public void init() {
            LOG.info("fileWriter start init()");
            this.writerSliceConfig = this.getPluginJobConf();
            //this.validateParameter();
            String dateFormatOld = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FORMAT);
            String dateFormatNew = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.DATE_FORMAT);
            if (null == dateFormatNew) {
                this.writerSliceConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.DATE_FORMAT,
                                dateFormatOld);
            }
            if (null != dateFormatOld) {
                LOG.warn("您使用format配置日期格式化, 这是不推荐的行为, 请优先使用dateFormat配置项, 两项同时存在则使用dateFormat.");
            }
            UnstructuredStorageWriterUtil
                    .validateParameter(this.writerSliceConfig);
            LOG.info("fileWriter end init()");
        }

        private void validateParameter() {
            this.writerSliceConfig
                    .getNecessaryValue(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                            TxtFileWriterErrorCode.REQUIRED_VALUE);

            String path = this.writerSliceConfig.getNecessaryValue(Key.PATH,
                    TxtFileWriterErrorCode.REQUIRED_VALUE);

            try {
                // warn: 这里用户需要配一个目录
                File dir = new File(path);
                if (dir.isFile()) {
                    throw DataXException
                            .asDataXException(
                                    TxtFileWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                            path));
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw DataXException
                                .asDataXException(
                                        TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                        String.format("您指定的文件路径 : [%s] 创建失败.",
                                                path));
                    }
                }
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件路径 : [%s] ", path), se);
            }
        }

        @Override
        public void prepare() {
            LOG.info("fileWriter start prepare()");
            String path = this.writerSliceConfig.getString(Key.PATH);
            String fileName = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
            String writeMode = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);
            // truncate option handler
            if ("truncate".equals(writeMode)) {
                LOG.info(String.format(
                        "由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的内容",
                        path, fileName));
                File dir = new File(path);
                // warn:需要判断文件是否存在，不存在时，不能删除
                try {
                    if (dir.exists()) {
                        // warn:不要使用FileUtils.deleteQuietly(dir);
                        FilenameFilter filter = new PrefixFileFilter(fileName);
                        File[] filesWithFileNamePrefix = dir.listFiles(filter);
                        for (File eachFile : filesWithFileNamePrefix) {
                            LOG.info(String.format("delete file [%s].",
                                    eachFile.getName()));
                            FileUtils.forceDelete(eachFile);
                        }
                        // FileUtils.cleanDirectory(dir);
                    }
                } catch (NullPointerException npe) {
                    throw DataXException
                            .asDataXException(
                                    TxtFileWriterErrorCode.Write_FILE_ERROR,
                                    String.format("您配置的目录清空时出现空指针异常 : [%s]",
                                            path), npe);
                } catch (IllegalArgumentException iae) {
                    throw DataXException.asDataXException(
                            TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您配置的目录参数异常 : [%s]", path));
                } catch (SecurityException se) {
                    throw DataXException.asDataXException(
                            TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您没有权限查看目录 : [%s]", path));
                } catch (IOException e) {
                    throw DataXException.asDataXException(
                            TxtFileWriterErrorCode.Write_FILE_ERROR,
                            String.format("无法清空目录 : [%s]", path), e);
                }
            } else if ("append".equals(writeMode)) {
                LOG.info(String
                        .format("由于您配置了writeMode append, 写入前不做清理工作, [%s] 目录下写入相应文件名前缀  [%s] 的文件",
                                path, fileName));
            } else if ("nonConflict".equals(writeMode)) {
                LOG.info(String.format(
                        "由于您配置了writeMode nonConflict, 开始检查 [%s] 下面的内容", path));
                // warn: check two times about exists, mkdirs
                File dir = new File(path);
                try {
                    if (dir.exists()) {
                        if (dir.isFile()) {
                            throw DataXException
                                    .asDataXException(
                                            TxtFileWriterErrorCode.ILLEGAL_VALUE,
                                            String.format(
                                                    "您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                                    path));
                        }
                        // fileName is not null
                        FilenameFilter filter = new PrefixFileFilter(fileName);
                        File[] filesWithFileNamePrefix = dir.listFiles(filter);
                        if (filesWithFileNamePrefix.length > 0) {
                            List<String> allFiles = new ArrayList<String>();
                            for (File eachFile : filesWithFileNamePrefix) {
                                allFiles.add(eachFile.getName());
                            }
                            LOG.error(String.format("冲突文件列表为: [%s]",
                                    StringUtils.join(allFiles, ",")));
                            throw DataXException
                                    .asDataXException(
                                            TxtFileWriterErrorCode.ILLEGAL_VALUE,
                                            String.format(
                                                    "您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.",
                                                    path));
                        }
                    } else {
                        boolean createdOk = dir.mkdirs();
                        if (!createdOk) {
                            throw DataXException
                                    .asDataXException(
                                            TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                            String.format(
                                                    "您指定的文件路径 : [%s] 创建失败.",
                                                    path));
                        }
                    }
                } catch (SecurityException se) {
                    throw DataXException.asDataXException(
                            TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您没有权限查看目录 : [%s]", path));
                }
            } else {
                throw DataXException
                        .asDataXException(
                                TxtFileWriterErrorCode.ILLEGAL_VALUE,
                                String.format(
                                        "仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 : [%s]",
                                        writeMode));
            }
            LOG.info("fileWriter end prepare()");
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            String filePrefix = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);

            Set<String> allFiles = new HashSet<String>();
            String path = null;
            try {
                path = this.writerSliceConfig.getString(Key.PATH);
                File dir = new File(path);
                allFiles.addAll(Arrays.asList(dir.list()));
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限查看目录 : [%s]", path));
            }

            String fileSuffix;
            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same file name

                Configuration splitedTaskConfig = this.writerSliceConfig
                        .clone();

                String fullFileName = null;
                //fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                fullFileName = String.format("%s_%s_%s-%s",
                    com.alibaba.datax.plugin.writer.txtfilewriter.StringUtils.formatDate(new Date()), filePrefix, System.currentTimeMillis(),
                    com.alibaba.datax.plugin.writer.txtfilewriter.StringUtils.numFormat(i, 5)).toUpperCase();
                while (allFiles.contains(fullFileName)) {
                    fileSuffix = UUID.randomUUID().toString().replace('-', 'a');
                    fullFileName = String.format("%s_%s_%s-%s",
                        com.alibaba.datax.plugin.writer.txtfilewriter.StringUtils.formatDate(new Date()), filePrefix, System.currentTimeMillis(),fileSuffix).toUpperCase();
                }
                fullFileName = fullFileName + ".nb";
                allFiles.add(fullFileName);

                splitedTaskConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                                fullFileName);

                LOG.info(String.format("splited write file name:[%s]",
                        fullFileName));

                writerSplitConfigs.add(splitedTaskConfig);
            }
            LOG.info("end do split.");
            return writerSplitConfigs;
        }

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String path;

        private String finalPath;

        private String fileName;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.path = this.writerSliceConfig.getString(Key.PATH);
            this.finalPath = this.writerSliceConfig.getString(Key.FINAL_PATH);
            this.fileName = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
        }

        @Override
        public void prepare() {
            this.writerSliceConfig
                .getNecessaryValue(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                    TxtFileWriterErrorCode.REQUIRED_VALUE);

            String path = this.writerSliceConfig.getNecessaryValue(Key.PATH,
                TxtFileWriterErrorCode.REQUIRED_VALUE);

            try {
                // warn: 这里用户需要配一个目录
                File dir = new File(path);
                if (dir.isFile()) {
                    throw DataXException
                        .asDataXException(
                            TxtFileWriterErrorCode.ILLEGAL_VALUE,
                            String.format(
                                "您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                path));
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw DataXException
                            .asDataXException(
                                TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                String.format("您指定的文件路径 : [%s] 创建失败.",
                                    path));
                    }
                }
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                    TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                    String.format("您没有权限创建文件路径 : [%s] ", path), se);
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("begin do write...");
            String fileFullPath = this.buildFilePath();
            LOG.info(String.format("write to file : [%s]", fileFullPath));

            OutputStream outputStream = null;
            File newFile = null;
            try {
                newFile = new File(fileFullPath);
                newFile.createNewFile();
                outputStream = new FileOutputStream(newFile);
                UnstructuredStorageWriterUtil.writeToStream(lineReceiver,
                        outputStream, this.writerSliceConfig, this.fileName,
                        this.getTaskPluginCollector());
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件  : [%s]", this.fileName));
            } catch (IOException ioe) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.Write_FILE_IO_ERROR,
                        String.format("无法创建待写文件 : [%s]", this.fileName), ioe);
            } finally {
                IOUtils.closeQuietly(outputStream);
                if (null != newFile && 0 == newFile.length()) {
                    LOG.info("任务执行成功，但无数据输出，空文件[{}]将被删除！", newFile.getName());
                    newFile.delete();
                } else {
                    try {
                        FileUtils.moveFile(newFile, new File(this.finalPath + "/" + newFile.getName()));
                        LOG.info("文件:[{}]移动到目录:[{}]成功！", newFile.getName(), this.finalPath);
                    } catch (IOException e) {
                        LOG.error("文件:[{}]移动到目录:[{}]失败！", newFile.getName(), this.finalPath, e);
                    }

                    //boolean b =
                    //    newFile.renameTo(new File(this.finalPath + "/" + newFile.getName()));
                    //if (b) {
                    //    LOG.info("文件:[{}]移动到目录:[{}]成功！", newFile.getName(), this.finalPath);
                    //} else {
                    //    LOG.error("文件:[{}]移动到目录:[{}]失败！", newFile.getName(), this.finalPath);
                    //}
                }
            }
            LOG.info("end do write");
        }

        private String buildFilePath() {
            boolean isEndWithSeparator = false;
            switch (IOUtils.DIR_SEPARATOR) {
            case IOUtils.DIR_SEPARATOR_UNIX:
                isEndWithSeparator = this.path.endsWith(String
                        .valueOf(IOUtils.DIR_SEPARATOR));
                break;
            case IOUtils.DIR_SEPARATOR_WINDOWS:
                isEndWithSeparator = this.path.endsWith(String
                        .valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
                break;
            default:
                break;
            }
            if (!isEndWithSeparator) {
                this.path = this.path + IOUtils.DIR_SEPARATOR;
            }
            return String.format("%s%s", this.path, this.fileName);
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }
}
