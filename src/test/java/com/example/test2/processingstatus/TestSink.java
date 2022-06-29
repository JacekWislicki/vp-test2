package com.example.test2.processingstatus;

import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.example.test2.processingstatus.model.ProcessingStatusAggregate;

import lombok.Data;

@Data
class TestSink implements SinkFunction<ProcessingStatusAggregate> {

    private static final long serialVersionUID = 1L;

    private static final Map<String, ProcessingStatusAggregate> result = new ConcurrentHashMap<>();
    private static final List<ProcessingStatusAggregate> resultWithNoKey = new Vector<>();

    @Override
    public void invoke(ProcessingStatusAggregate value, Context context) throws Exception {
        if (!ProcessingStatusAggregate.EMPTY_AGGREGATE_KEY.equals(value.getKey())) {
            result.put(value.getKey(), value);
        } else {
            resultWithNoKey.add(value);
        }
    }

    Map<String, ProcessingStatusAggregate> getResult() {
        return result;
    }

    List<ProcessingStatusAggregate> getResultWithNoKey() {
        return resultWithNoKey;
    }

    void clear() {
        result.clear();
        resultWithNoKey.clear();
    }
}
