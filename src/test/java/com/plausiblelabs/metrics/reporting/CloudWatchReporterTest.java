package com.plausiblelabs.metrics.reporting;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.google.common.collect.Sets;
import com.codahale.metrics.*;

import static junit.framework.Assert.assertEquals;

public class CloudWatchReporterTest {

    // Use a separate registry for each test to keep the metrics apart
    private MetricRegistry testRegistry = new MetricRegistry();
    private DummyCloudWatchClient client = new DummyCloudWatchClient();
    private CloudWatchReporter.Enabler enabler =
        new CloudWatchReporter.Enabler("testnamespace", client).withRegistry(testRegistry);

    @Test
    public void defaultSentMetrics() throws IOException, InterruptedException {
        CloudWatchReporter reporter = enabler.build();
        reporter.report();
        assertEquals(0, client.putData.size());
    }

    @Test
    public void instanceIdDimension() throws IOException, InterruptedException {
        Counter counter = testRegistry.counter("TestCounter");
        CloudWatchReporter reporter = enabler.withInstanceIdDimension("flask").build();
        reporter.report();
        assertEquals(1, client.putData.size());
        for (MetricDatum datum : client.putData) {
            assertEquals(1, datum.getDimensions().size());
            assertEquals("InstanceId", datum.getDimensions().get(0).getName());
            assertEquals("flask", datum.getDimensions().get(0).getValue());
        }
    }

    @Test
    public void disablingDefaults() throws IOException, InterruptedException {
        assertEquals(0, client.putData.size());
    }

    @Test
    public void timer() {
        enabler
            .withFiveMinuteRate(true)
            .withOneMinuteRate(false)
            .withTimerSummary(true)
            .withPercentiles(.1, .5, .9, .999);
        Timer timer = testRegistry.timer("TestTimer");
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 50; j++) {
                timer.update(i, TimeUnit.MINUTES);
            }
        }
        CloudWatchReporter reporter = enabler.build();
        reporter.report();
        assertEquals(9, client.putData.size());
        assertEquals(Sets.newHashSet("TestTimer.median",
                                     "TestTimer_percentile_0.999",
                                     "TestTimer_percentile_0.9",
                                     "TestTimer.mean",
                                     "TestTimer.5MinuteRate",
                                     "TestTimer.min",
                                     "TestTimer.max",
                                     "TestTimer.stddev",
                                     "TestTimer_percentile_0.1"),
                     client.latestPutByName.keySet());
        MetricDatum min = client.latestPutByName.get("TestTimer.min");
        assertEquals("The recorded minutes were converted to milliseconds for CloudWatch", StandardUnit.Milliseconds.toString(), min.getUnit());
        assertEquals(0.0, min.getValue());
        MetricDatum percentile999 = client.latestPutByName.get("TestTimer_percentile_0.999");
        assertEquals("The recorded minutes were converted to milliseconds for CloudWatch", 5940000.0, percentile999.getValue());
    }

    @Test
    public void counter() {
        Counter counter = testRegistry.counter("TestCounter");
        CloudWatchReporter reporter = enabler.build();
        reporter.report();
        assertEquals(1, client.putData.size());
        assertEquals(0.0, client.putData.get(0).getValue());
        assertEquals(StandardUnit.Count.toString(), client.putData.get(0).getUnit());
        counter.inc();
        client.putData.clear();
        reporter.report();
        assertEquals(1.0, client.putData.get(0).getValue());

    }
}
