package com.alibaba.datax.core;

/**
 * @Author nkong
 * @Date 2020/6/20 13:59
 * @Version 1.0
 **/
public class TestEngine {

    public static void main(String[] args) {
        System.out.println("aaa13dfa".toUpperCase());
        System.setProperty("datax.home", "target/datax/datax");
        String[] dataxArgs = {"-job", "aaa_job/d.json", "-mode", "standalone", "-jobid","-1"};
        try {
            Engine.entry(dataxArgs);
        } catch (Throwable e) {
            System.out.println(e.getMessage());
        }
    }

}
