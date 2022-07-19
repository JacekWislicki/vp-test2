package com.example.test2.flinkjob;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

import com.example.test2.flinkjob.model.PulsarMessage2;
import com.example.test2.flinkjob.utils.Config;

import lombok.Data;

@Data
public class SimpleJob2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        PulsarSource<PulsarMessage2> statusSource = createPulsarSource(Config.IN_TOPIC_STATUS_NAME, Config.IN_TOPIC_STATUS_SUB);
        new SimpleJob2(statusSource).build(environment);
        environment.execute();
    }

    private static PulsarSource<PulsarMessage2> createPulsarSource(String topic, String subscription) {
        PulsarSourceBuilder<PulsarMessage2> builder = PulsarSource.builder()
            .setAdminUrl(Config.ADMI_URL)
            .setServiceUrl(Config.SERVICE_URL)
            .setStartCursor(StartCursor.earliest())
            .setTopics(topic)
            .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(Schema.AVRO(PulsarMessage2.class), PulsarMessage2.class))
            .setSubscriptionName(subscription)
            .setSubscriptionType(SubscriptionType.Exclusive);
        return builder.build();
    }

    private final Source<PulsarMessage2, ?, ?> pulsarSource;

    void build(StreamExecutionEnvironment environment) {
        DataStream<PulsarMessage2> pulsarStream =
            environment.fromSource(pulsarSource, WatermarkStrategy.noWatermarks(), "pulsar-source", TypeInformation.of(PulsarMessage2.class));

        pulsarStream.addSink(new PrintSinkFunction<>());
    }
}
