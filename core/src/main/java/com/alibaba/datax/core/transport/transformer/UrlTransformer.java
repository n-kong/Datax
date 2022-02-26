package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/8/3
 */
public class UrlTransformer extends Transformer {

    private static final Logger logger = LoggerFactory.getLogger(UrlTransformer.class);

    public UrlTransformer() {
        setTransformerName("dx_url2base64");
    }

    @Override public Record evaluate(Record record, Object... paras) {

        try {
            if (paras.length != 1) {
                throw new RuntimeException("dx_url2base64 paras must be 1");
            }
        } catch (Exception e) {
            throw DataXException
                .asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
                    "paras:" + Arrays
                        .asList(paras).toString() + " => " + e.getMessage());
        }

        int columnIndex = (Integer) paras[0];
        Column column = record.getColumn(columnIndex);
        try {
            String oriValue = column.asString();
            //如果字段为空，跳过subStr处理
            if (oriValue == null) {
                return record;
            }
            oriValue = image2Base64(oriValue);
            record.setColumn(columnIndex, new StringColumn(oriValue));
        } catch (Exception e) {
            throw DataXException
                .asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),
                    e);
        }

        return record;
    }

    private String image2Base64(String imageUrl) {
        URL url = null;
        InputStream is = null;
        HttpURLConnection urlConnection = null;
        ByteArrayOutputStream outputStream = null;
        try {
            url = new URL(imageUrl);
            urlConnection = (HttpURLConnection) url.openConnection();
            //urlConnection.connect();
            urlConnection.setConnectTimeout(5000);
            is = urlConnection.getInputStream();
            outputStream = new ByteArrayOutputStream();
            byte[] bytes = new byte[1024];
            int len = 0;
            while ((len = is.read(bytes)) != -1) {
                outputStream.write(bytes, 0, len);
            }
            return encode(outputStream.toByteArray());
        } catch (Exception e) {
            logger.error("transform image url to base64 error. mag:{}", e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(outputStream);
            IOUtils.closeQuietly(is);
            if (null != urlConnection) {
                urlConnection.disconnect();
            }
        }
        return imageUrl;
    }

    private String encode(byte[] str) {
        String reg = "[\n-\r]";
        Pattern compile = Pattern.compile(reg);
        Matcher matcher = compile.matcher(new BASE64Encoder().encode(str));
        return matcher.replaceAll("");
    }
}
