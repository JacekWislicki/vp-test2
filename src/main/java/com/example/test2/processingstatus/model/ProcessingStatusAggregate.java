package com.example.test2.processingstatus.model;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ProcessingStatusAggregate {

    public static final String EMPTY_STATUS_KEY = "-";
    public static final String EMPTY_AGGREGATE_KEY = "-:-";

    private Set<ProcessingStatus> statuses = new HashSet<>();

    public ProcessingStatusAggregate(Collection<ProcessingStatus> statuses) {
        addAll(statuses);
    }

    public void addAll(Collection<ProcessingStatus> statuses) {
        this.statuses.addAll(statuses);
    }

    public String getKey() {
        String urn = null;
        String vin = null;
        for (ProcessingStatus status : statuses) {
            if (StringUtils.isNotBlank(status.getUrn())) {
                urn = status.getUrn();
            }
            if (StringUtils.isNotBlank(status.getVin())) {
                vin = status.getVin();
            }
            if (StringUtils.isNoneBlank(urn, vin)) {
                break;
            }
        }
        return Objects.toString(urn, EMPTY_STATUS_KEY) + ":" + Objects.toString(vin, EMPTY_STATUS_KEY);
    }

    @Override
    public String toString() {
        return getKey() + " " + statuses.stream().map(ProcessingStatus::toString).collect(Collectors.joining("\n\t", "\n\t", ""));
    }
}
