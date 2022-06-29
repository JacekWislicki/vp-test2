package com.example.test2.processingstatus;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.flink.connector.testframe.source.FromElementsSource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.example.test2.processingstatus.model.ProcessingStatus;
import com.example.test2.processingstatus.model.ProcessingStatus.Status;
import com.example.test2.processingstatus.model.ProcessingStatusAggregate;
import com.example.test2.processingstatus.model.Stage;

import lombok.Data;

@RunWith(Parameterized.class)
public class ProcessingStatusAggregatorJobJUnit4Test {

    private final Parameter parameter;

    public ProcessingStatusAggregatorJobJUnit4Test(Parameter parameter) {
        this.parameter = parameter;
    }

    private StreamExecutionEnvironment environment;
    private TestSink sink = new TestSink();

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(4)
                .setNumberTaskManagers(4)
                .build()
        );

    @Before
    public void setUp() {
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @After
    public void tearDown() {
        sink.clear();
    }

    @Test
    public void testJob() throws Exception {
        FromElementsSource<ProcessingStatus> source = new FromElementsSource<>(parameter.getStatuses());
        ProcessingStatusAggregatorJob job = new ProcessingStatusAggregatorJob(source, sink);
        job.build(environment);
        environment.execute();

        Map<String, ProcessingStatusAggregate> result = sink.getResult();
        List<ProcessingStatusAggregate> resultWithNoKey = sink.getResultWithNoKey();
        for (ProcessingStatusAggregate aggregate : parameter.getAggregates()) {
            String failedMessage = "Assertion failed for " + parameter;
            if (!ProcessingStatusAggregate.EMPTY_AGGREGATE_KEY.equals(aggregate.getKey())) {
                assertEquals(aggregate, result.get(aggregate.getKey()), failedMessage);
            } else {
                assertTrue(failedMessage, resultWithNoKey.contains(aggregate));
            }
        }
    }

    @Data
    private static class Parameter {

        private final List<ProcessingStatus> statuses;
        private final List<ProcessingStatusAggregate> aggregates;
    }

    @Parameters
    public static List<Parameter> getParameters() {
        // urn1
        ProcessingStatus status1 = new ProcessingStatus("vin1", "urn1", Stage.COC_TVV_PULSAR, Status.OK, "status1", null);
        ProcessingStatus status2 = new ProcessingStatus("vin1", null, Stage.COC_TVV_S3, Status.OK, "status2", null);
        ProcessingStatus status3 = new ProcessingStatus(null, "urn1", Stage.DOMTOM_VEHICLE_CREATION_INPUT_DECORATOR, Status.OK, "status3", null);
        ProcessingStatus status4 = new ProcessingStatus("vin1", null, Stage.DOMTOM_VEHICLE_CREATION_INPUT_VALIDATOR, Status.OK, "status4", null);

        // urn2
        ProcessingStatus status5 = new ProcessingStatus(null, "urn2", Stage.SPIL_VEHICLE_CREATION_INPUT_DECORATOR, Status.OK, "status5", null);
        ProcessingStatus status6 = new ProcessingStatus("vin2", null, Stage.SPIL_VEHICLE_CREATION_INPUT_FILTER, Status.OK, "status6", null);
        ProcessingStatus status7 = new ProcessingStatus("vin2", "urn2", Stage.SPIL_VEHICLE_CREATION_INPUT_ROUTER, Status.OK, "status7", null);

        // no URN
        List<Parameter> parameters = new ArrayList<>();
        ProcessingStatus status8 = new ProcessingStatus("vin3", null, Stage.DOMTOM_VEHICLE_CREATION_INPUT_DECORATOR, Status.OK, "status8", null);
        ProcessingStatus status9 = new ProcessingStatus("vin4", null, Stage.DOMTOM_VEHICLE_CREATION_INPUT_VALIDATOR, Status.OK, "status9", null);
        ProcessingStatus status10 = new ProcessingStatus("vin3", null, Stage.ONREQUEST_VEHICLE_CREATION_INPUT_DECORATOR, Status.OK, "status10", null);

        // no URN and VIN
        ProcessingStatus status11 = new ProcessingStatus(null, null, Stage.TEST_COC_TVV_S3, Status.OK, "status11", null);
        ProcessingStatus status12 = new ProcessingStatus(null, null, Stage.TEST_SPIL_VEHICLE_CREATION_S3, Status.OK, "status12", null);
        ProcessingStatus status13 = new ProcessingStatus(null, null, Stage.VEHICLE_CREATION_WLTP_ENRICHMENT, Status.OK, "status13", null);

        parameters.add(
            new Parameter(
                Arrays.asList(status1),
                Arrays.asList(
                    new ProcessingStatusAggregate(Arrays.asList(status1))
                )
            )
        );
        parameters.add(
            new Parameter(
                Arrays.asList(status1, status2),
                Arrays.asList(
                    new ProcessingStatusAggregate(Arrays.asList(status1, status2))
                )
            )
        );
        parameters.add(
            new Parameter(
                Arrays.asList(status1, status2, status3),
                Arrays.asList(
                    new ProcessingStatusAggregate(Arrays.asList(status1, status2, status3))
                )
            )
        );
        parameters.add(
            new Parameter(
                Arrays.asList(status1, status2, status3, status4),
                Arrays.asList(
                    new ProcessingStatusAggregate(Arrays.asList(status1, status2, status3, status4))
                )
            )
        );

        parameters.add(
            new Parameter(
                Arrays.asList(status1, status2, status3, status4, status5, status6, status7),
                Arrays.asList(
                    new ProcessingStatusAggregate(Arrays.asList(status1, status2, status3, status4)),
                    new ProcessingStatusAggregate(Arrays.asList(status5, status6, status7))
                )
            )
        );

        parameters.add(
            new Parameter(
                Arrays.asList(status1, status2, status3, status4, status5, status6, status7, status8, status9, status10),
                Arrays.asList(
                    new ProcessingStatusAggregate(Arrays.asList(status1, status2, status3, status4)),
                    new ProcessingStatusAggregate(Arrays.asList(status5, status6, status7)),
                    new ProcessingStatusAggregate(Arrays.asList(status8, status10)),
                    new ProcessingStatusAggregate(Arrays.asList(status9))
                )
            )
        );

        parameters.add(
            new Parameter(
                Arrays.asList(status1, status2, status3, status4, status5, status6, status7, status8, status9, status10, status11, status12,
                    status13),
                Arrays.asList(
                    new ProcessingStatusAggregate(Arrays.asList(status1, status2, status3, status4)),
                    new ProcessingStatusAggregate(Arrays.asList(status5, status6, status7)),
                    new ProcessingStatusAggregate(Arrays.asList(status8, status10)),
                    new ProcessingStatusAggregate(Arrays.asList(status9)),
                    new ProcessingStatusAggregate(Arrays.asList(status11)),
                    new ProcessingStatusAggregate(Arrays.asList(status12)),
                    new ProcessingStatusAggregate(Arrays.asList(status13))
                )
            )
        );

        return parameters;
    }
}
