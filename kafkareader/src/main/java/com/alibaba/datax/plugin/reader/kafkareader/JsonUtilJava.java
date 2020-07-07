package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;

public class JsonUtilJava {
    public static HashMap<String ,Object> parseJsonStrToMap(String str){
        return JSON.parseObject(str, new HashMap<String,Object>().getClass());
    }
}
