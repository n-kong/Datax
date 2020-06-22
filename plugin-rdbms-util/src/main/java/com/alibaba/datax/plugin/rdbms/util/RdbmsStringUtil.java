package com.alibaba.datax.plugin.rdbms.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author nkong
 * @Date 2020/6/22 21:23
 * @Version 1.0
 **/
public class RdbmsStringUtil {

    private static Pattern PATTERN = Pattern.compile("\t|\r|\n");

    public static synchronized String replaceBlank(String inStr) {

        String outStr = "";
            if (null != inStr && !"null".equalsIgnoreCase(inStr)) {
                Matcher m = PATTERN.matcher(inStr);
                outStr = m.replaceAll("");
            }

        return outStr;
    }


}
