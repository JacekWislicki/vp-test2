package com.example.test2.processingstatus.function;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.example.test2.processingstatus.model.ProcessingStatus;
import com.example.test2.processingstatus.model.ProcessingStatusAggregate;

public class ProcessingStatusMergeKeyedProcessFunction extends KeyedProcessFunction<String, ProcessingStatus, ProcessingStatusAggregate> {

    private static final long serialVersionUID = 1L;

    private transient ListState<ProcessingStatus> statusesState;

    @Override
    public void open(Configuration parameters) throws Exception {
        statusesState = getRuntimeContext().getListState(new ListStateDescriptor<>("statuses", ProcessingStatus.class));
    }

    @Override
    public void processElement(ProcessingStatus value, KeyedProcessFunction<String, ProcessingStatus, ProcessingStatusAggregate>.Context context,
        Collector<ProcessingStatusAggregate> out) throws Exception {
        ProcessingStatusAggregate statusAggregate = new ProcessingStatusAggregate();
        if (!context.getCurrentKey().equals(ProcessingStatusAggregate.EMPTY_STATUS_KEY)) {
            List<ProcessingStatus> statuses = StreamSupport.stream(statusesState.get().spliterator(), false).collect(Collectors.toList());
            statuses.add(value);
            statusesState.update(statuses);

            statusAggregate.addAll(statuses);
        } else {
            statusAggregate.addAll(Arrays.asList(value));
        }
        out.collect(statusAggregate);
    }
}