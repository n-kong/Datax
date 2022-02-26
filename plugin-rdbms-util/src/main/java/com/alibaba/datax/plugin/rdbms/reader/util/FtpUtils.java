package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.exception.DataXException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

/**
 * @author nkong
 */
public class FtpUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FtpUtils.class);
    private static FTPClient ftpClient = null;

    private static void initFtpClient(String host, String port, String user, String password) {
        try {
            ftpClient = new FTPClient();
            ftpClient.connect(host, Integer.parseInt(port));
            ftpClient.login(user, password);
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            // 设置被动模式，防止在Linux上由于安全限制可能某些端口没开启导致阻塞
            ftpClient.enterLocalPassiveMode();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        processFtp("ftp://data_transfer_kxsj:data_transfer_kxsj@15.118.123.53:21/data3/kxsj/test/bbb.jpg");
        //String str = "/ad/asd/hfdh.jpg";
        //System.out.println(str.substring(str.lastIndexOf("/") + 1));
        //loadResource("file_writer/final");
        write();
    }
    private static void write() {
        try {
            BufferedWriter writer =
                new BufferedWriter(new FileWriter(new File("file_writer/final/20200820.chk"), true));
            writer.write("retert.jpg" + "\n");
            writer.write("uuiyuiyuiy.jpg" + "\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<String> list = new ArrayList();
    private static void loadResource(String filePath){

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        File file = new File(filePath + "/" + sdf.format(new Date()) + ".chk");
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

            try {

                String item = null;
                while ((item = bufferedReader.readLine()) != null) {
                    list.add(item);
                }



            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println(list.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static String processFtp(String value) {

        String user = value.split("//")[1].split(":")[0];
        String password = value.split(":")[2].split("@")[0];
        String host = value.split("@")[1].split(":")[0];
        String port = value.split("@")[1].split(":")[1].split("/")[0];
        String dir = value.substring(value.indexOf("/", 7), value.lastIndexOf("/"));
        String file = value.substring(value.lastIndexOf("/") + 1);

        if (null == ftpClient || !ftpClient.isConnected()) {
            initFtpClient(host, port, user, password);
        }

        InputStream inputStream = null;
        ByteArrayOutputStream outputStream = null;
        String base64 = "";
        try {
            //是否成功登录服务器
            int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                LOG.error("login ftp error...");
                ftpClient.disconnect();
            }

            //跳转到指定目录
            ftpClient.changeWorkingDirectory(dir);
            inputStream = ftpClient.retrieveFileStream(new String(file.getBytes("gbk"), "ISO-8859-1"));
            if (inputStream != null) {
                byte[] data = null;
                int len = 0;
                outputStream = new ByteArrayOutputStream();
                data = new byte[1024];
                while (-1 != (len = inputStream.read(data))) {
                    outputStream.write(data, 0, len);
                }
                //while((rc = inputStream.read(buff,0,100))>0){
                //    byteArrayOutputStream.write(buff,0,rc);
                //}
                data = outputStream.toByteArray();

                BASE64Encoder encoder = new BASE64Encoder();
                base64 = encoder.encode(data);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            try {
                if (null != outputStream) {
                    outputStream.flush();
                    outputStream.close();
                }
                if (null != inputStream) {
                    inputStream.close();
                    ftpClient.completePendingCommand();
                }
                //if (ftpClient.isConnected()) {
                //    ftpClient.disconnect();
                //}
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        return base64.replace("\r\n", "");
    }
}
