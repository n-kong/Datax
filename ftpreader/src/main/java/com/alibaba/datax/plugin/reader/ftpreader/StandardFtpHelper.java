package com.alibaba.datax.plugin.reader.ftpreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPCmd;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

public class StandardFtpHelper extends FtpHelper {
    private static final Logger LOG = LoggerFactory.getLogger(StandardFtpHelper.class);
    FTPClient ftpClient = null;
    HashSet<String> sourceFiles = new HashSet<String>();

    @Override
    public void loginFtpServer(String host, String username, String password, int port, int timeout,
        String connectMode) {
        ftpClient = new FTPClient();
        try {
            // 连接
            ftpClient.connect(host, port);
            // 登录
            ftpClient.login(username, password);
            // 不需要写死ftp server的OS TYPE,FTPClient getSystemType()方法会自动识别
            // ftpClient.configure(new FTPClientConfig(FTPClientConfig.SYST_UNIX));
            ftpClient.setConnectTimeout(timeout);
            ftpClient.setDataTimeout(timeout);
            if ("PASV".equals(connectMode)) {
                ftpClient.enterRemotePassiveMode();
                ftpClient.enterLocalPassiveMode();
            } else if ("PORT".equals(connectMode)) {
                ftpClient.enterLocalActiveMode();
                // ftpClient.enterRemoteActiveMode(host, port);
            }
            int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftpClient.disconnect();
                String message = String.format("与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
                    "message:host =" + host + ",username = " + username + ",port =" + port);
                LOG.error(message);
                throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message);
            }
            //设置命令传输编码
            String fileEncoding = System.getProperty("file.encoding");
            ftpClient.setControlEncoding(fileEncoding);
        } catch (UnknownHostException e) {
            String message = String.format("请确认ftp服务器地址是否正确，无法连接到地址为: [%s] 的ftp服务器", host);
            LOG.error(message);
            throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
        } catch (IllegalArgumentException e) {
            String message = String.format("请确认连接ftp服务器端口是否正确，错误的端口: [%s] ", port);
            LOG.error(message);
            throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
        } catch (Exception e) {
            String message = String.format("与ftp服务器建立连接失败 : [%s]",
                "message:host =" + host + ",username = " + username + ",port =" + port);
            LOG.error(message);
            throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
        }
    }

    @Override
    public void logoutFtpServer() {
        if (ftpClient.isConnected()) {
            try {
                //todo ftpClient.completePendingCommand();//打开流操作之后必须，原因还需要深究
                ftpClient.logout();
            } catch (IOException e) {
                String message = "与ftp服务器断开连接失败";
                LOG.error(message);
                throw DataXException
                    .asDataXException(FtpReaderErrorCode.FAIL_DISCONNECT, message, e);
            } finally {
                if (ftpClient.isConnected()) {
                    try {
                        ftpClient.disconnect();
                    } catch (IOException e) {
                        String message = "与ftp服务器断开连接失败";
                        LOG.error(message);
                        throw DataXException
                            .asDataXException(FtpReaderErrorCode.FAIL_DISCONNECT, message, e);
                    }
                }
            }
        }
    }

    @Override
    public boolean isDirExist(String directoryPath) {
        try {
            return ftpClient.changeWorkingDirectory(
                new String(directoryPath.getBytes(), FTP.DEFAULT_CONTROL_ENCODING));
        } catch (IOException e) {
            String message = String.format("进入目录：[%s]时发生I/O异常,请确认与ftp服务器的连接正常", directoryPath);
            LOG.error(message);
            throw DataXException
                .asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
        }
    }

    @Override
    public boolean isFileExist(String filePath) {
        boolean isExitFlag = false;
        try {
            FTPFile[] ftpFiles =
                ftpClient.listFiles(new String(filePath.getBytes(), FTP.DEFAULT_CONTROL_ENCODING));
            if (ftpFiles.length == 1 && ftpFiles[0].isFile()) {
                isExitFlag = true;
            }
        } catch (IOException e) {
            String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
            LOG.error(message);
            throw DataXException
                .asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
        }
        return isExitFlag;
    }

    @Override
    public boolean isSymbolicLink(String filePath) {
        boolean isExitFlag = false;
        try {
            FTPFile[] ftpFiles =
                ftpClient.listFiles(new String(filePath.getBytes(), FTP.DEFAULT_CONTROL_ENCODING));
            if (ftpFiles.length == 1 && ftpFiles[0].isSymbolicLink()) {
                isExitFlag = true;
            }
        } catch (IOException e) {
            String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
            LOG.error(message);
            throw DataXException
                .asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
        }
        return isExitFlag;
    }

    @Override
    public HashSet<String> getListFiles(String directoryPath, int parentLevel,
        int maxTraversalLevel) {
        if (parentLevel < maxTraversalLevel) {
            String parentPath = null;// 父级目录,以'/'结尾
            int pathLen = directoryPath.length();
            if (directoryPath.contains("*") || directoryPath.contains("?")) {
                // path是正则表达式
                String subPath =
                    UnstructuredStorageReaderUtil.getRegexPathParentPath(directoryPath);
                if (isDirExist(subPath)) {
                    parentPath = subPath;
                } else {
                    String message =
                        String.format("不能进入目录：[%s]," + "请确认您的配置项path:[%s]存在，且配置的用户有权限进入", subPath,
                            directoryPath);
                    LOG.error(message);
                    throw DataXException
                        .asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
                }
            } else if (isDirExist(directoryPath)) {
                // path是目录
                if (directoryPath.charAt(pathLen - 1) == IOUtils.DIR_SEPARATOR || directoryPath.charAt(pathLen - 1) == IOUtils.DIR_SEPARATOR_UNIX) {
                    parentPath = directoryPath;
                } else {
                    parentPath = directoryPath + IOUtils.DIR_SEPARATOR;
                }
            } else if (isFileExist(directoryPath)) {
                // path指向具体文件
                sourceFiles.add(directoryPath);
                return sourceFiles;
            } else if (isSymbolicLink(directoryPath)) {
                //path是链接文件
                String message = String.format("文件:[%s]是链接文件，当前不支持链接文件的读取", directoryPath);
                LOG.error(message);
                throw DataXException.asDataXException(FtpReaderErrorCode.LINK_FILE, message);
            } else {
                String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", directoryPath);
                LOG.error(message);
                System.exit(0);
                //throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
            }

            try {
                //FTPFile[] fs = ftpClient
                //    .listFiles(new String(directoryPath.getBytes(), FTP.DEFAULT_CONTROL_ENCODING));
                FTPFile[] fs = ftpClient.listFiles(directoryPath);
                for (FTPFile ff : fs) {
                    String strName = ff.getName();
                    String filePath = parentPath + strName;
                    if (ff.isDirectory()) {
                        if (!(strName.equals(".") || strName.equals(".."))) {
                            parentLevel += 1;
                            if (parentLevel < maxTraversalLevel) {
                                //递归处理
                                getListFiles(filePath, parentLevel, maxTraversalLevel);
                            }
                        }
                    } else if (ff.isFile()) {
                        // 是文件
                        sourceFiles.add(filePath);
                    } else if (ff.isSymbolicLink()) {
                        //是链接文件
                        String message = String.format("文件:[%s]是链接文件，当前不支持链接文件的读取", filePath);
                        LOG.error(message);
                        throw DataXException
                            .asDataXException(FtpReaderErrorCode.LINK_FILE, message);
                    } else {
                        String message = String.format("请确认path:[%s]存在，且配置的用户有权限读取", filePath);
                        LOG.error(message);
                        throw DataXException
                            .asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
                    }
                } // end for FTPFile
            } catch (IOException e) {
                String message =
                    String.format("获取path：[%s] 下文件列表时发生I/O异常,请确认与ftp服务器的连接正常", directoryPath);
                LOG.error(message);
                throw DataXException
                    .asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
            }
            return sourceFiles;
        } else {
            //超出最大递归层数
            String message = String
                .format("获取path：[%s] 下文件列表时超出最大层数,请确认路径[%s]下不存在软连接文件", directoryPath,
                    directoryPath);
            LOG.error(message);
            throw DataXException
                .asDataXException(FtpReaderErrorCode.OUT_MAX_DIRECTORY_LEVEL, message);
        }
    }

    @Override
    public InputStream getInputStream(String filePath) {
        try {
            return ftpClient
                .retrieveFileStream(new String(filePath.getBytes(), FTP.DEFAULT_CONTROL_ENCODING));
        } catch (IOException e) {
            String message =
                String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filePath, filePath);
            LOG.error(message);
            throw DataXException.asDataXException(FtpReaderErrorCode.OPEN_FILE_ERROR, message);
        }
    }

    @Override public String getFtpImage(String filePath) {

        InputStream inputStream = null;
        ByteArrayOutputStream outputStream = null;
        String base64 = "";
        try {
            inputStream = ftpClient
                .retrieveFileStream(new String(filePath.getBytes(), FTP.DEFAULT_CONTROL_ENCODING));
            if (inputStream != null) {
                byte[] data = new byte[1024];
                int len = 0;
                outputStream = new ByteArrayOutputStream();
                while (-1 != (len = inputStream.read(data))) {
                    outputStream.write(data, 0, len);
                }
                outputStream.flush();
                data = outputStream.toByteArray();
                BASE64Encoder encoder = new BASE64Encoder();
                base64 = encoder.encode(data);
            }
        } catch (IOException e) {
            String message =
                String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filePath, filePath);
            LOG.error(message);
        } finally {
            try {
                if (null != outputStream) {
                    outputStream.close();
                }
                if (null != inputStream) {
                    inputStream.close();
                    ftpClient.completePendingCommand();
                }
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }

        return base64.replace("\r", "").replace("\n", "");
    }

    @Override
    public void downFile(List<String> files, String outTmpPath, String outPath) {

        FileOutputStream outputStream = null;
        for (String file : files) {
            String fileName = new File(file).getName();
            File tmpFile = new File(outTmpPath + fileName);
            try {
                outputStream = new FileOutputStream(tmpFile);
                boolean flg = ftpClient
                    .retrieveFile(new String(file.getBytes(), FTP.DEFAULT_CONTROL_ENCODING),
                        outputStream);
                if (flg) {
                    LOG.info("文件：[{}]下载成功.", file);
                } else {
                    LOG.info("文件：[{}]下载失败.", file);
                }
            } catch (FileNotFoundException e) {
                LOG.error("File:[{}] not found.", file, e);
            } catch (IOException e2) {
                LOG.error(e2.getMessage(), e2);
            } finally {
                IOUtils.closeQuietly(outputStream);
                try {
                    FileUtils.moveFile(tmpFile, new File(outPath + fileName));
                    LOG.info("文件:[{}]移动到目录:[{}]成功！", fileName, outPath);
                } catch (IOException e) {
                    LOG.error("文件:[{}]移动到目录:[{}]失败！", fileName, outPath, e);
                }
            }
        }
    }

    @Override
    public void deleteFile(String fileName) {
        try {
            boolean b = ftpClient.deleteFile(fileName);
            if (b) {
                LOG.info("文件：[{}]删除成功.", fileName);
            } else {
                if (ftpClient.deleteFile(new String(fileName.getBytes(), "iso-8859-1"))) {
                    LOG.info("文件：[{}]删除成功.", fileName);
                } else {
                    LOG.info("文件：[{}]删除失败.", fileName);
                }
            }
        } catch (IOException e) {
            LOG.error("delete file error，error msg: {}", e.getMessage(), e);
            e.printStackTrace();
        }
    }

    @Override
    public void renameFile(String fileName, String targetFile) {
        try {
            ftpClient.sendCommand(FTPCmd.RENAME_FROM, fileName);
            ftpClient.sendCommand(FTPCmd.RENAME_TO, targetFile);
            //使用rename方法时移动文件失败，查看由于此时的ftp的Reply状态码为350,只执行了RENAME_FROM,没有执行RENAME_TO
            //ftpClient.rename(sourceFile,targetFile);
        } catch (IOException e) {
            LOG.error("rename file error，error msg: {}", e.getMessage(), e);
            e.printStackTrace();
        }
    }

    @Override
    public void mkDirIfNotExist(String path) {
        try {
            if (!isDirExist(path)) {
                ftpClient.mkd(path);
            }
        } catch (IOException e) {
            LOG.error("创建文件夹：[{}]失败，请您手动创建，确保文件夹存在！", path, e);
            e.printStackTrace();
        }
    }
}
