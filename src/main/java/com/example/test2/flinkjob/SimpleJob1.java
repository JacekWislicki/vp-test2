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

import com.example.test2.flinkjob.model.PulsarMessage1;
import com.example.test2.flinkjob.utils.Config;

import lombok.Data;

@Data
public class SimpleJob1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        PulsarSource<PulsarMessage1> statusSource = createPulsarSource(Config.IN_TOPIC_STATUS_NAME, Config.IN_TOPIC_STATUS_SUB);
        new SimpleJob1(statusSource).build(environment);
        environment.execute();
    }

    private static PulsarSource<PulsarMessage1> createPulsarSource(String topic, String subscription) {
        PulsarSourceBuilder<PulsarMessage1> builder = PulsarSource.builder()
            .setAdminUrl(Config.ADMI_URL)
            .setServiceUrl(Config.SERVICE_URL)
            .setStartCursor(StartCursor.earliest())
            .setTopics(topic)
            .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(Schema.AVRO(PulsarMessage1.class), PulsarMessage1.class))
            .setSubscriptionName(subscription)
            .setSubscriptionType(SubscriptionType.Exclusive);
        return builder.build();
    }

    private final Source<PulsarMessage1, ?, ?> pulsarSource;

    void build(StreamExecutionEnvironment environment) {
        DataStream<PulsarMessage1> pulsarStream =
            environment.fromSource(pulsarSource, WatermarkStrategy.noWatermarks(), "pulsar-source", TypeInformation.of(PulsarMessage1.class));

        pulsarStream.addSink(new PrintSinkFunction<>());
    }
}
