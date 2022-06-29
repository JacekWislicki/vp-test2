package com.example.test2.processingstatus.function;

import java.io.IOException;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import com.example.test2.processingstatus.model.ProcessingStatusAggregate;

public class ProcessingStatusAggregateMergeKeyedCoProcessFunction
    extends KeyedCoProcessFunction<String, ProcessingStatusAggregate, ProcessingStatusAggregate, ProcessingStatusAggregate> {

    private static final long serialVersionUID = 1L;

    private transient ValueState<ProcessingStatusAggregate> statusAggregateState;

    @Override
    public void open(Configuration parameters) throws Exception {
        statusAggregateState = getRuntimeContext().getState(new ValueStateDescriptor<>("statuses", ProcessingStatusAggregate.class));
    }

    @Override
    public void processElement1(ProcessingStatusAggregate value,
        KeyedCoProcessFunction<String, ProcessingStatusAggregate, ProcessingStatusAggregate, ProcessingStatusAggregate>.Context context,
        Collector<ProcessingStatusAggregate> out) throws Exception {
        merge(value, out);
    }

    @Override
    public void processElement2(ProcessingStatusAggregate value,
        KeyedCoProcessFunction<String, ProcessingStatusAggregate, ProcessingStatusAggregate, ProcessingStatusAggregate>.Context context,
        Collector<ProcessingStatusAggregate> out) throws Exception {
        merge(value, out);
    }

    private void merge(ProcessingStatusAggregate value, Collector<ProcessingStatusAggregate> out) throws IOException {
        ProcessingStatusAggregate statusAggregate = statusAggregateState.value();
        if (statusAggregate == null) {
            statusAggregate = new ProcessingStatusAggregate();
        }
        statusAggregate.addAll(value.getStatuses());
        statusAggregateState.update(statusAggregate);

        out.collect(statusAggregate);
    }
}