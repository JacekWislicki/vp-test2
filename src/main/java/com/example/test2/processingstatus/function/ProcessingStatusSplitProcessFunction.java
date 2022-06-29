package com.example.test2.processingstatus.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.example.test2.processingstatus.model.ProcessingStatus;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ProcessingStatusSplitProcessFunction extends ProcessFunction<ProcessingStatus, ProcessingStatus> {

    private static final long serialVersionUID = 1L;

    private final OutputTag<ProcessingStatus> withUrnOnly = new OutputTag<>("with-urn-only") {

        private static final long serialVersionUID = 1L;
    };
    private final OutputTag<ProcessingStatus> withVinOnly = new OutputTag<>("with-vin-only") {

        private static final long serialVersionUID = 1L;
    };
    private final OutputTag<ProcessingStatus> withoutUrnOrVin = new OutputTag<>("without-unr-or-vin") {

        private static final long serialVersionUID = 1L;
    };

    @Override
    public void processElement(ProcessingStatus value, ProcessFunction<ProcessingStatus, ProcessingStatus>.Context context,
        Collector<ProcessingStatus> out) throws Exception {
        String urn = value.getUrn();
        String vin = value.getVin();
        if (StringUtils.isNoneBlank(urn, vin)) {
            out.collect(value);
        } else if (StringUtils.isNotBlank(urn)) {
            context.output(withUrnOnly, value);
        } else if (StringUtils.isNotBlank(vin)) {
            context.output(withVinOnly, value);
        } else {
            context.output(withoutUrnOrVin, value);
        }
    }
}
