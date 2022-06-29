package com.example.test2;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import com.example.test2.processingstatus.utils.Config;

public class PulsarSink<OUT> implements SinkFunction<OUT>, AutoCloseable {

    private static final long serialVersionUID = 1L;

    private final transient PulsarClient client;
    private final transient Producer<OUT> producer;

    public PulsarSink(Class<OUT> outClass, String topic) {
        try {
            this.client = PulsarClient.builder().serviceUrl(Config.SERVICE_URL).build();
            this.producer = client.newProducer(Schema.AVRO(outClass)).topic(topic).create();
        } catch (Exception e) {
            throw new RuntimeException("Error on connecting to Pulsar", e);
        }
    }

    @Override
    public void invoke(OUT value, Context context) throws Exception {
        producer.send(value);
    }

    @Override
    public void close() {
        try {
            producer.close();
            client.close();
        } catch (PulsarClientException e) {
            throw new RuntimeException("Error on shutdown", e);
        }
    }
}
