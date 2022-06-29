package com.example.test2.processingstatus.utils;

public class Config {

    public static final String SERVICE_URL = "pulsar://localhost:6650";
    public static final String ADMI_URL = "http://localhost:8080";

    public static final String IN_TOPIC_STATUS_NAME = "persistent://public/default/test-status-in";
    public static final String IN_TOPIC_STATUS_SUB = "test-status-in-flink";

    public static final String OUT_TOPIC_AGGR_NAME = "persistent://public/default/test-aggr-out";

    private Config() {}
}
