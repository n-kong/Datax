package com.alibaba.datax.core;

public class TestEngine {

    public static void main(String[] args) {

        System.setProperty("datax.home", "target/datax/datax");

        String[] config = {"-mode", "standalone", "-jobid", "-1", "-job", "job/filetofile.json"};
        try {
            Engine.entry(config);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
