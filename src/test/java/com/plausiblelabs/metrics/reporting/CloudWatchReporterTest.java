package com.plausiblelabs.metrics.reporting;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.google.common.collect.Sets;
import com.codahale.metrics.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class CloudWatchReporterTest {

    // Use a separate registry for each test to keep the metrics apart
    private MetricRegistry testRegistry = new MetricRegistry();
    private DummyCloudWatchClient client = new DummyCloudWatchClient();
    private CloudWatchReporter.Enabler enabler =
        new CloudWatchReporter.Enabler("testnamespace", client).withRegistry(testRegistry);

    @Test
    public void testDefaultSentMetrics() throws IOException, InterruptedException {
        enabler.build().start(1, TimeUnit.MILLISECONDS);
        assertEquals(2, client.putData.size());
        for (MetricDatum datum : client.putData) {
            assertTrue(datum.getDimensions().isEmpty());
            assertTrue(datum.getMetricName().startsWith("jvm.memory"));
        }
    }

    @Test
    public void testInstanceIdDimension() throws IOException, InterruptedException {
        enabler.withInstanceIdDimension("flask").build().start(1, TimeUnit.MILLISECONDS);
        assertEquals(2, client.putData.size());
        for (MetricDatum datum : client.putData) {
            assertEquals(1, datum.getDimensions().size());
            assertEquals("InstanceId", datum.getDimensions().get(0).getName());
            assertEquals("flask", datum.getDimensions().get(0).getValue());
        }
    }

    @Test
    public void testDisablingDefaults() throws IOException, InterruptedException {
        assertEquals(0, client.putData.size());
    }

    @Test
    public void testTimer() {
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
        enabler.build().start(1L, TimeUnit.MINUTES);
        assertEquals(9, client.putData.size());
        assertEquals(Sets.newHashSet("com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer.median",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer_percentile_0.999",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer_percentile_0.9",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer.mean",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer.5MinuteRate",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer.min",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer.max",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer.stddev",
                                     "com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer_percentile_0.1"),
                     client.latestPutByName.keySet());
        MetricDatum min = client.latestPutByName.get("com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer.min");
        assertEquals("The recorded minutes were converted to seconds for CloudWatch", StandardUnit.Seconds.toString(), min.getUnit());
        assertEquals(0.0, min.getValue());
        MetricDatum percentile999 = client.latestPutByName.get("com.plausiblelabs.metrics.reporting.CloudWatchReporterTest.TestTimer_percentile_0.999");
        assertEquals("The recorded minutes were converted to seconds for CloudWatch", 5940.0, percentile999.getValue());
    }

    @Test
    public void testCounter() {
        Counter counter = testRegistry.counter("TestCounter");
        CloudWatchReporter reporter = enabler.build();
        reporter.start(1L, TimeUnit.MILLISECONDS);
        assertEquals(1, client.putData.size());
        assertEquals(0.0, client.putData.get(0).getValue());
        assertEquals(StandardUnit.Count.toString(), client.putData.get(0).getUnit());
        counter.inc();
        client.putData.clear();
        reporter.start(1L, TimeUnit.MILLISECONDS);
        assertEquals(1.0, client.putData.get(0).getValue());

    }
}
