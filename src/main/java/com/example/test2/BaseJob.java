package com.example.test2;

import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

import com.example.test2.processingstatus.utils.Config;

public abstract class BaseJob {

    protected static final <OUT> SinkFunction<OUT> createPulsarSink(String topic, Class<OUT> outClass) {
        return new PulsarSink<>(outClass, topic);
    }

    protected static final <IN> PulsarSource<IN> createPulsarSource(String topic, String subscription, Class<IN> inClass) {
        PulsarSourceBuilder<IN> builder = PulsarSource.builder()
            .setAdminUrl(Config.ADMI_URL)
            .setServiceUrl(Config.SERVICE_URL)
            .setStartCursor(StartCursor.earliest())
            .setTopics(topic)
            .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(Schema.AVRO(inClass), inClass))
            .setSubscriptionName(subscription)
            .setSubscriptionType(SubscriptionType.Exclusive);
        return builder.build();
    }

    protected BaseJob() {}
}
