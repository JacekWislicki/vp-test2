package com.example.test2.processingstatus;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.example.test2.BaseJob;
import com.example.test2.processingstatus.function.ProcessingStatusAggregateMergeKeyedCoProcessFunction;
import com.example.test2.processingstatus.function.ProcessingStatusMergeKeyedProcessFunction;
import com.example.test2.processingstatus.function.ProcessingStatusSplitProcessFunction;
import com.example.test2.processingstatus.model.ProcessingStatus;
import com.example.test2.processingstatus.model.ProcessingStatusAggregate;
import com.example.test2.processingstatus.utils.Config;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ProcessingStatusAggregatorJob extends BaseJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        PulsarSource<ProcessingStatus> statusSource =
            createPulsarSource(Config.IN_TOPIC_STATUS_NAME, Config.IN_TOPIC_STATUS_SUB, ProcessingStatus.class);
        SinkFunction<ProcessingStatusAggregate> aggregateSink = createPulsarSink(Config.OUT_TOPIC_AGGR_NAME, ProcessingStatusAggregate.class);
        new ProcessingStatusAggregatorJob(statusSource, aggregateSink).build(environment);
        environment.execute();
    }

    private final Source<ProcessingStatus, ?, ?> statusSource;
    private final SinkFunction<ProcessingStatusAggregate> aggregateSink;

    private final KeySelector<ProcessingStatus, String> statusKeySelector = value -> {
        String urn = value.getUrn();
        String vin = value.getVin();
        if (StringUtils.isNoneBlank(urn, vin)) {
            return urn + ":" + vin;
        } else if (StringUtils.isNotBlank(urn)) {
            return urn;
        } else if (StringUtils.isNotBlank(vin)) {
            return vin;
        }
        return ProcessingStatusAggregate.EMPTY_STATUS_KEY;
    };
    private final KeySelector<ProcessingStatusAggregate, String> withUrnStatusAggregateKeySelector = value -> value.getKey().split(":")[0];
    private final KeySelector<ProcessingStatusAggregate, String> withVinStatusAggregateKeySelector = value -> value.getKey().split(":")[1];

    void build(StreamExecutionEnvironment environment) {
        DataStream<ProcessingStatus> inputStatusStream = environment
            .fromSource(statusSource, WatermarkStrategy.noWatermarks(), "status-source", TypeInformation.of(ProcessingStatus.class));

        ProcessingStatusSplitProcessFunction splitProcessFunction = new ProcessingStatusSplitProcessFunction();
        SingleOutputStreamOperator<ProcessingStatus> processedStatusStream = inputStatusStream.process(splitProcessFunction);

        ProcessingStatusMergeKeyedProcessFunction statusMergeFunction = new ProcessingStatusMergeKeyedProcessFunction();
        KeyedStream<ProcessingStatusAggregate, String> withUrnAndVinStatusAggregateStream = processedStatusStream
            .keyBy(statusKeySelector)
            .process(statusMergeFunction)
            .keyBy(withUrnStatusAggregateKeySelector);
        KeyedStream<ProcessingStatusAggregate, String> withUrnOnlyStatusAggregateStream = processedStatusStream
            .getSideOutput(splitProcessFunction.getWithUrnOnly())
            .keyBy(statusKeySelector)
            .process(statusMergeFunction)
            .keyBy(withUrnStatusAggregateKeySelector);
        KeyedStream<ProcessingStatusAggregate, String> withVinOnlyStatusAggregateStream = processedStatusStream
            .getSideOutput(splitProcessFunction.getWithVinOnly())
            .keyBy(statusKeySelector)
            .process(statusMergeFunction)
            .keyBy(withVinStatusAggregateKeySelector);

        KeyedStream<ProcessingStatusAggregate, String> withoutUrnOrVinStatusAggregateStream = processedStatusStream
            .getSideOutput(splitProcessFunction.getWithoutUrnOrVin())
            .keyBy(statusKeySelector)
            .process(statusMergeFunction)
            .keyBy(withVinStatusAggregateKeySelector);
        withoutUrnOrVinStatusAggregateStream.addSink(aggregateSink);

        ProcessingStatusAggregateMergeKeyedCoProcessFunction statusAggregateMergeFunction =
            new ProcessingStatusAggregateMergeKeyedCoProcessFunction();
        SingleOutputStreamOperator<ProcessingStatusAggregate> statusAggregateResultStream = withUrnAndVinStatusAggregateStream
            .connect(withUrnOnlyStatusAggregateStream)
            .process(statusAggregateMergeFunction)
            .connect(withVinOnlyStatusAggregateStream)
            .keyBy(withVinStatusAggregateKeySelector, withVinStatusAggregateKeySelector)
            .process(statusAggregateMergeFunction);

        statusAggregateResultStream.addSink(aggregateSink);
    }
}
