package com.plausiblelabs.metrics.reporting;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Reports metrics to <a href="http://aws.amazon.com/cloudwatch/">Amazon's CloudWatch</a> periodically.
 */
public class CloudWatchReporter extends ScheduledReporter {

    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchReporter.class.getSimpleName());

    /**
     * Amazon's docs say they don't accept values smaller than 1E-130, but experimentally 1E-108 is the smallest
     * accepted value. Metric values smaller than this will be trimmed to this value and a debug log will be printed the
     * first time it happens in the reporter.
     */
    static final double SMALLEST_SENDABLE = 1E-108;

    /**
     * Amazon's docs say they don't accept values larger than 1E116, but experimentally 1E108 is the smallest
     * accepted value. Metric values larger than this will be trimmed to this value and a debug log will be printed the
     * first time it happens in the reporter.
     */
    static final double LARGEST_SENDABLE = 1E108;

    /**
     * <p>Creates or starts a CloudWatchReporter.</p>
     * <p>As CloudWatch charges 50 cents per unique metric, this reporter attempts to be parsimonious with the values
     * it sends by default. It only sends the median, 95th, and 99th percentiles for histograms and timers, and the
     * one minute rate for meters and timers. Additional metrics may be sent through configuring this class.</p>
     */
    public static class Enabler {
        private final String namespace;
        private final AmazonCloudWatchClient client;
        private final List<DimensionAdder> dimensionAdders = new ArrayList<DimensionAdder>();

        private MetricRegistry registry;
        private MetricFilter filter = MetricFilter.ALL;

        private TimeUnit durationUnit;

        private TimeUnit rateUnit;

        private boolean sendToCloudWatch = true;

        private double[] percentilesToSend = { .5, .95, .99 };

        private boolean sendOneMinute = true, sendFiveMinute, sendFifteenMinute;

        private boolean sendMeterSummary;

        private boolean sendTimerLifetime;

        private boolean sendHistoLifetime;

        /**
         * Creates an Enabler that sends values in the given namespace to the given AWS account
         *
         * @param namespace the namespace. Must be non-null and not empty.
         */
        public Enabler(String namespace, AWSCredentialsProvider awsCredentialsProvider) {
            this(namespace, new AmazonCloudWatchClient(awsCredentialsProvider));
        }

        /**
         * Creates an Enabler that sends values in the given namespace using the given client
         *
         * @param namespace the namespace. Must be non-null and not empty.
         */
        public Enabler(String namespace, AmazonCloudWatchClient client) {
            this.namespace = namespace;
            this.client = client;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
        }

        /**
         * <p>The histogram and meter percentiles to send. If <code>.5</code> is included, it'll be reported as
         * <code>median</code>.This defaults to <code>.5, .95, and .99</code>.
         * @param percentiles the percentiles to send. Replaces the currently set percentiles.
         * @return this Enabler.
         */
        public Enabler withPercentiles(double...percentiles) {
            this.percentilesToSend = percentiles;
            return this;
        }

        /**
         * If the one minute rate should be sent for meters and timers. Enabled by default.
         * @param enabled if the rate should be sent.
         * @return this Enabler.
         */
        public Enabler withOneMinuteRate(boolean enabled) {
            this.sendOneMinute = enabled;
            return this;
        }


        /**
         * If the five minute rate should be sent for meters and timers. Disabled by default.
         * @param enabled if the rate should be sent.
         * @return this Enabler.
         */
        public Enabler withFiveMinuteRate(boolean enabled) {
            this.sendFiveMinute = enabled;
            return this;
        }

        /**
         * If the fifteen minute rate should be sent for meters and timers. Disabled by default.
         * @param enabled if the rate should be sent.
         * @return this Enabler.
         */
        public Enabler withFifteenMinuteRate(boolean enabled) {
            this.sendFifteenMinute = enabled;
            return this;
        }

        /**
         * If the lifetime value summary should be sent for meters ie count and mean rate. Disabled by default.
         * @param enabled if the summary should be sent.
         * @return this Enabler.
         */
        public Enabler withMeterSummary(boolean enabled) {
            this.sendMeterSummary = enabled;
            return this;
        }

        /**
         * If the lifetime value summary should be sent for timers ie min, max, mean, and count. Disabled by default.
         * @param enabled if the summary should be sent.
         * @return this Enabler.
         */
        public Enabler withTimerSummary(boolean enabled) {
            this.sendTimerLifetime = enabled;
            return this;
        }

        /**
         * If the lifetime value summary should be sent for histograms ie min, max, mean, and count. Disabled by default.
         * @param enabled if the summary should be sent.
         * @return this Enabler.
         */
        public Enabler withHistogramSummary(boolean enabled) {
            this.sendHistoLifetime = enabled;
            return this;
        }

        /**
         * Use the given registry to fetch metrics. Defaults to <code>Metrics.defaultRegistry()</code>
         * @return this Enabler.
         */
        public Enabler withRegistry(MetricRegistry registry) {
            this.registry = registry;
            return this;
        }


        /**
         * Filters to metrics to send through the given predicate. Only matching metrics will be sent.
         * @return this Enabler.
         */
        public Enabler withPredicate(MetricFilter predicate) {
            this.filter = predicate;
            return this;
        }



        /**
         * <p>Adds an <code>InstanceId</code> dimension to all sent metrics with EC2 instance's id. The id isfetched
         * from the EC2 metadata server at <code>http://169.254.169.254/latest/meta-data/instance-id</code>.</p>
         *
         * <p>This will only work if running inside EC2. If used outside of EC2, or if the service fails, an
         * <code>InstanceId</code> dimenson with the value <code>unknown</code> will be sent.</p>
         *
         * @return this Enabler.
         */
        public Enabler withEC2InstanceIdDimension() {
            return withEC2InstanceIdDimension(MetricFilter.ALL);
        }

        /**
         * <p>Adds an <code>InstanceId</code> dimension to all metrics matching the given predicate. The instance id
         * fetched from the EC2 metadata server at <code>http://169.254.169.254/latest/meta-data/instance-id</code>.</p>
         *
         * <p>This will only work if running inside EC2. If used outside of EC2, or if the service fails, an
         * <code>InstanceId</code> dimenson with the value <code>unknown</code> will be sent.</p>
         *
         * @return this Enabler.
         */
        public Enabler withEC2InstanceIdDimension(MetricFilter predicate) {
            return withDimensionAdder(new InstanceIdAdder(predicate));
        }

        /**
         * <p>Adds an <code>InstanceId</code> dimension to all metrics with the given instance id as a value.</p>
         *
         * @return this Enabler.
         */
        public Enabler withInstanceIdDimension(String instanceId) {
            return withInstanceIdDimension(instanceId, MetricFilter.ALL);
        }

        /**
         * <p>Adds an <code>InstanceId</code> dimension to all metrics matching the given predicate with the given
         * instance id as a value.</p>
         *
         * @return this Enabler.
         */
        public Enabler withInstanceIdDimension(String instanceId, MetricFilter predicate) {
            return withDimensionAdder(new InstanceIdAdder(predicate, instanceId));
        }

        /**
         * Runs the given adder on all sent metrics. Note: a single metric may have a maximum of 10 dimensions.
         *
         * @return this Enabler.
         */
        public Enabler withDimensionAdder(DimensionAdder adder) {
            this.dimensionAdders.add(adder);
            return this;
        }

        /**
         * If metrics will be sent to CloudWatch. Enabled by default. If disabled, the metrics that would be sent are
         * logged instead. It's useful to disable CloudWatch and see if the expected metrics are being sent before
         * incurring the monthly charge.
         *
         * @return this Enabler.
         */
        public Enabler withCloudWatchEnabled(boolean enabled) {
            sendToCloudWatch = enabled;
            return this;
        }

        /**
         * Creates a reporter with the settings currently configured on this enabler.
         */
        public CloudWatchReporter build() {
            return new CloudWatchReporter(registry, namespace, client, filter, rateUnit, durationUnit, dimensionAdders,
                    percentilesToSend, sendOneMinute, sendFiveMinute, sendFifteenMinute,
                    sendMeterSummary, sendTimerLifetime, sendHistoLifetime, sendToCloudWatch);
        }

        /**
         * Starts a CloudWatchReporter with the settings currently configured on this enabler at the period set on it.
         * @param period to poll in given {@code unit}
         * @param unit at which to poll
         */
        public void enable(long period, TimeUnit unit) {
            try {
                build().start(period, unit);
            } catch (Exception e) {
                LOG.error("Error creating/starting CloudWatch reporter:", e);
            }
        }
    }

    private final List<DimensionAdder> dimensionAdders;

    private final Set<String> unsendable = new HashSet<String>();

    private final String namespace;

    private final AmazonCloudWatchClient client;

    private final double[] percentilesToSend;

    private final boolean sendOneMinute, sendFiveMinute, sendFifteenMinute;

    private final boolean sendMeterSummary;

    private final boolean sendTimerLifetime;

    private final boolean sendHistoLifetime;

    private final boolean sendToCloudWatch;

    private final Clock clock;

    private final StandardUnit standardDurationUnit;

    private CloudWatchReporter(MetricRegistry registry, String namespace, AmazonCloudWatchClient client,
                               MetricFilter predicate, TimeUnit rateUnit, TimeUnit durationUnit, List<DimensionAdder> dimensionAdders,
                               double[] percentilesToSend, boolean sendOneMinute,
                               boolean sendFiveMinute, boolean sendFifteenMinute, boolean sendMeterSummary,
                               boolean sendTimerLifetime, boolean sendHistoLifetime, boolean sendToCloudWatch) {
        super(registry, "cloudwatch-reporter", predicate, rateUnit, durationUnit);
        this.namespace = namespace;
        this.client = client;
        this.dimensionAdders = dimensionAdders;
        this.clock = Clock.defaultClock();
        this.standardDurationUnit = convertDurationUnit(durationUnit);
        this.percentilesToSend = percentilesToSend;
        this.sendOneMinute = sendOneMinute;
        this.sendFiveMinute = sendFiveMinute;
        this.sendFifteenMinute = sendFifteenMinute;
        this.sendMeterSummary = sendMeterSummary;
        this.sendTimerLifetime = sendTimerLifetime;
        this.sendHistoLifetime = sendHistoLifetime;
        this.sendToCloudWatch = sendToCloudWatch;
    }

    @Override @SuppressWarnings("rawtypes")
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        PutMetricDataRequest putMetricDataRequest = new PutMetricDataRequest().withNamespace(namespace);
        final Date context = new Date(clock.getTime());
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            putMetricDataRequest = processGauge(putMetricDataRequest, entry.getKey(), entry.getValue(), context);
        }
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            putMetricDataRequest = processCounter(putMetricDataRequest, entry.getKey(), entry.getValue(), context);
        }
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            putMetricDataRequest = processHistogram(putMetricDataRequest, entry.getKey(), entry.getValue(), context);
        }
        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            putMetricDataRequest = processMeter(putMetricDataRequest, entry.getKey(), entry.getValue(), context);
        }
        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            putMetricDataRequest = processTimer(putMetricDataRequest, entry.getKey(), entry.getValue(), context);
        }
        try {
            sendToCloudWatch(putMetricDataRequest);
        } catch (Exception e) {
            LOG.error("Error writing to CloudWatch: {}", e.getMessage());
        }
    }

    private void sendToCloudWatch(PutMetricDataRequest putMetricDataRequest) {
        try {
            if (sendToCloudWatch && !putMetricDataRequest.getMetricData().isEmpty()) {
                client.putMetricData(putMetricDataRequest);
            }
        } catch (RuntimeException re) {
            LOG.warn("Failed writing to CloudWatch: {}", re.getMessage());
            LOG.warn("  failed CloudWatch put request: {}", putMetricDataRequest);
            throw re;
        }
    }

    private boolean sentTooSmall, sentTooLarge;

    private PutMetricDataRequest sendValue(PutMetricDataRequest putMetricDataRequest, Date timestamp, String name, double value, StandardUnit unit, List<Dimension> dimensions) {
        double absValue = Math.abs(value);
        if (absValue < SMALLEST_SENDABLE) {
            if (absValue > 0) {// Allow 0 through untouched, everything else gets rounded to SMALLEST_SENDABLE
                if (value < 0) {
                    value = -SMALLEST_SENDABLE;
                } else {
                    value = SMALLEST_SENDABLE;
                }
                if (!sentTooSmall) {
                    LOG.debug("Value for {} is smaller than what CloudWatch supports; trimming to {}. Further small values won't be logged.", name, value);
                    sentTooSmall = true;
                }
            }
        } else if (absValue > LARGEST_SENDABLE) {
            if (value < 0) {
                value = -LARGEST_SENDABLE;
            } else {
                value = LARGEST_SENDABLE;
            }
            if (!sentTooLarge) {
                LOG.debug("Value for {} is larger than what CloudWatch supports; trimming to {}. Further large values won't be logged.", name, value);
                sentTooLarge = true;
            }
        }
        // TODO limit to 10 dimensions
        MetricDatum datum = new MetricDatum()
                .withTimestamp(timestamp)
                .withValue(value)
                .withMetricName(name)
                .withDimensions(dimensions)
                .withUnit(unit);

        LOG.debug("Sending to CloudWatch {}", datum);
        putMetricDataRequest.withMetricData(datum);

        // Can only send 20 metrics at a time
        if (putMetricDataRequest.getMetricData().size() == 20) {
            sendToCloudWatch(putMetricDataRequest);
            putMetricDataRequest = new PutMetricDataRequest().withNamespace(namespace);
        }
        return putMetricDataRequest;
    }

    private List<Dimension> createDimensions(String name, Metric metric) {
        List<Dimension> dimensions = new ArrayList<Dimension>();
        for (DimensionAdder adder : dimensionAdders) {
            dimensions.addAll(adder.generate(name, metric));
        }
        return dimensions;
    }

    public PutMetricDataRequest processGauge(PutMetricDataRequest putMetricDataRequest, String name, Gauge<?> gauge, Date context) {
        if (gauge.getValue() instanceof Number) {
            return sendValue(putMetricDataRequest, context, name, ((Number) gauge.getValue()).doubleValue(), StandardUnit.None, createDimensions(name, gauge));
        } else if (unsendable.add(name)) {
            LOG.warn("The type of the value for {} is {}. It must be a subclass of Number to send to CloudWatch.", name, gauge.getValue().getClass());
        }
        return putMetricDataRequest;
    }

    public PutMetricDataRequest processCounter(PutMetricDataRequest putMetricDataRequest, String name, Counter counter, Date context) {
        return sendValue(putMetricDataRequest, context, name, counter.getCount(), StandardUnit.Count, createDimensions(name, counter));
    }

    public PutMetricDataRequest processMeter(PutMetricDataRequest putMetricDataRequest, String name, Metered meter, Date context) {
        List<Dimension> dimensions = createDimensions(name, meter);
        if (sendOneMinute) {
            putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".1MinuteRate", meter.getOneMinuteRate(), StandardUnit.None, dimensions);
        }
        if (sendFiveMinute) {
            putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".5MinuteRate", meter.getFiveMinuteRate(), StandardUnit.None, dimensions);
        }
        if (sendFifteenMinute) {
            putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".15MinuteRate", meter.getFifteenMinuteRate(), StandardUnit.None, dimensions);
        }
        if (sendMeterSummary) {
            putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".count", meter.getCount(), StandardUnit.Count, dimensions);
            putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".meanRate", meter.getMeanRate(), StandardUnit.None, dimensions);
        }
        return putMetricDataRequest;
    }

    public PutMetricDataRequest processHistogram(PutMetricDataRequest putMetricDataRequest, String name, Histogram histogram, Date context) {
        List<Dimension> dimensions = createDimensions(name, histogram);
        Snapshot snapshot = histogram.getSnapshot();
        for (double percentile : percentilesToSend) {
            if (percentile == .5) {
                putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".median", snapshot.getMedian(), StandardUnit.None, dimensions);
            } else {
                putMetricDataRequest = sendValue(putMetricDataRequest, context, name + "_percentile_" + percentile, snapshot.getValue(percentile), StandardUnit.None, dimensions);
            }
        }
        if (sendHistoLifetime) {
            putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".min", snapshot.getMin(), StandardUnit.None, dimensions);
            putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".max", snapshot.getMax(), StandardUnit.None, dimensions);
            putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".mean", snapshot.getMean(), StandardUnit.None, dimensions);
            putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".stddev", snapshot.getStdDev(), StandardUnit.None, dimensions);
        }
        return putMetricDataRequest;
    }

    public PutMetricDataRequest processTimer(PutMetricDataRequest putMetricDataRequest, String name, Timer timer, Date context) {
        putMetricDataRequest = processMeter(putMetricDataRequest, name, timer, context);
        List<Dimension> dimensions = createDimensions(name, timer);
        Snapshot snapshot = timer.getSnapshot();
        for (double percentile : percentilesToSend) {
            if (percentile == .5) {
                putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".median", convertDuration(snapshot.getMedian()), standardDurationUnit, dimensions);
            } else {
                putMetricDataRequest = sendValue(putMetricDataRequest, context, name + "_percentile_" + percentile, convertDuration(snapshot.getValue(percentile)), standardDurationUnit, dimensions);
            }
        }
        if (sendTimerLifetime) {
            putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".min", convertDuration(timer.getSnapshot().getMin()), standardDurationUnit, dimensions);
            putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".max", convertDuration(timer.getSnapshot().getMax()), standardDurationUnit, dimensions);
            putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".mean", convertDuration(timer.getSnapshot().getMean()), standardDurationUnit, dimensions);
            putMetricDataRequest = sendValue(putMetricDataRequest, context, name + ".stddev", convertDuration(timer.getSnapshot().getStdDev()), standardDurationUnit, dimensions);
        }
        return putMetricDataRequest;
    }

    private StandardUnit convertDurationUnit(TimeUnit durationUnit) {
        switch (durationUnit) {
            case MILLISECONDS:
                return StandardUnit.Milliseconds;
            case MICROSECONDS:
                return StandardUnit.Microseconds;
            case SECONDS:
                return StandardUnit.Seconds;
            case NANOSECONDS:
            case MINUTES:
            case HOURS:
            case DAYS:
            default:
                throw new IllegalArgumentException(String.format("AWS CloudWatch only supports milli/micro/seconds [ %s ]", durationUnit.name()));
        }
    }

}
