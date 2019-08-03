using Kafka.Common.Metrics;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Kafka.Common.MetricName;
using Kafka.Common.MetricNameTemplate;
using Kafka.Common.Utils.KafkaThread;
using Kafka.Common.Utils.Time;
using Kafka.Common.Utils.Utils;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kafka.Common.Metrics
{



/**
 * A registry of sensors and metrics.
 * <p>
 * A metric is a named, numerical measurement. A sensor is a handle to record numerical measurements as they occur. Each
 * Sensor has zero or more associated metrics. For example a Sensor might represent message sizes and we might associate
 * with this sensor a metric for the average, maximum, or other statistics computed off the sequence of message sizes
 * that are recorded by the sensor.
 * <p>
 * Usage looks something like this:
 *
 * <pre>
 * // set up metrics:
 * Metrics metrics = new Metrics(); // this is the global repository of metrics and sensors
 * Sensor sensor = metrics.sensor(&quot;message-sizes&quot;);
 * MetricName metricName = new MetricName(&quot;message-size-avg&quot;, &quot;producer-metrics&quot;);
 * sensor.add(metricName, new Avg());
 * metricName = new MetricName(&quot;message-size-max&quot;, &quot;producer-metrics&quot;);
 * sensor.add(metricName, new Max());
 *
 * // as messages are sent we record the sizes
 * sensor.record(messageSize);
 * </pre>
 */
public class Metrics : IDisposable
{

    private MetricConfig config;
    private ConcurrentDictionary<MetricName, KafkaMetric> metrics;
    private ConcurrentDictionary<string, Sensor> sensors;
    private ConcurrentDictionary<Sensor, List<Sensor>> childrenSensors;
    private List<MetricsReporter> reporters;
    private Time time;
    private ScheduledThreadPoolExecutor metricsScheduler;
    private static ILogger log = new LoggerFactory().CreateLogger<Metrics>();

    /**
     * Create a metrics repository with no metric reporters and default configuration.
     * Expiration of Sensors is disabled.
     */
    public Metrics() {
        this(new MetricConfig());
    }

    /**
     * Create a metrics repository with no metric reporters and default configuration.
     * Expiration of Sensors is disabled.
     */
    public Metrics(Time time) {
        this(new MetricConfig(), new ArrayList<MetricsReporter>(0), time);
    }

    /**
     * Create a metrics repository with no metric reporters and the given default configuration.
     * Expiration of Sensors is disabled.
     */
    public Metrics(MetricConfig defaultConfig, Time time) {
        this(defaultConfig, new ArrayList<MetricsReporter>(0), time);
    }


  /**
     * Create a metrics repository with no reporters and the given default config. This config will be used for any
     * metric that doesn't override its own config. Expiration of Sensors is disabled.
     * @param defaultConfig The default config to use for all metrics that don't override their config
     */
    public Metrics(MetricConfig defaultConfig) {
        this(defaultConfig, new ArrayList<MetricsReporter>(0), Time.SYSTEM);
    }

    /**
     * Create a metrics repository with a default config and the given metric reporters.
     * Expiration of Sensors is disabled.
     * @param defaultConfig The default config
     * @param reporters The metrics reporters
     * @param time The time instance to use with the metrics
     */
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time) {
        this(defaultConfig, reporters, time, false);
    }

    /**
     * Create a metrics repository with a default config, given metric reporters and the ability to expire eligible sensors
     * @param defaultConfig The default config
     * @param reporters The metrics reporters
     * @param time The time instance to use with the metrics
     * @param enableExpiration true if the metrics instance can garbage collect inactive sensors, false otherwise
     */
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time, bool enableExpiration) {
        this.config = defaultConfig;
        this.sensors = new ConcurrentHashMap<>();
        this.metrics = new ConcurrentHashMap<>();
        this.childrenSensors = new ConcurrentHashMap<>();
        this.reporters = Utils.notNull(reporters);
        this.time = time;
        for (MetricsReporter reporter : reporters)
            reporter.init(new ArrayList<KafkaMetric>());

        // Create the ThreadPoolExecutor only if expiration of Sensors is enabled.
        if (enableExpiration) {
            this.metricsScheduler = new ScheduledThreadPoolExecutor(1);
            // Creating a daemon thread to not block shutdown
            this.metricsScheduler.setThreadFactory(new ThreadFactory() {
                public Thread newThread(Runnable runnable) {
                    return KafkaThread.daemon("SensorExpiryThread", runnable);
                }
            });
            this.metricsScheduler.scheduleAtFixedRate(new ExpireSensorTask(), 30, 30, TimeUnit.SECONDS);
        } else {
            this.metricsScheduler = null;

    #region IDisposable Support
    private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~Metrics()
        // {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }

        addMetric(metricName("count", "kafka-metrics-count", "total number of registered metrics"),
            new Measurable() {
                @Override
                public double measure(MetricConfig config, long now) {
                    return metrics.size();
                }
            });
    }

    /**
     * Create a MetricName with the given name, group, description and tags, plus default tags specified in the metric
     * configuration. Tag in tags takes precedence if the same tag key is specified in the default metric configuration.
     *
     * @param name        The name of the metric
     * @param group       logical group name of the metrics to which this metric belongs
     * @param description A human-readable description to include in the metric
     * @param tags        additional key/value attributes of the metric
     */
    public MetricName metricName(string name, string group, string description, Dictionary<string, string> tags) {
        Dictionary<string, string> combinedTag = new LinkedHashMap<>(config.tags());
        combinedTag.putAll(tags);
        return new MetricName(name, group, description, combinedTag);
    }

    /**
     * Create a MetricName with the given name, group, description, and default tags
     * specified in the metric configuration.
     *
     * @param name        The name of the metric
     * @param group       logical group name of the metrics to which this metric belongs
     * @param description A human-readable description to include in the metric
     */
    public MetricName metricName(string name, string group, string description) {
        return metricName(name, group, description, new HashMap<string, string>());
    }

    /**
     * Create a MetricName with the given name, group and default tags specified in the metric configuration.
     *
     * @param name        The name of the metric
     * @param group       logical group name of the metrics to which this metric belongs
     */
    public MetricName metricName(string name, string group) {
        return metricName(name, group, "", new HashMap<string, string>());
    }

    /**
     * Create a MetricName with the given name, group, description, and keyValue as tags,  plus default tags specified in the metric
     * configuration. Tag in keyValue takes precedence if the same tag key is specified in the default metric configuration.
     *
     * @param name          The name of the metric
     * @param group         logical group name of the metrics to which this metric belongs
     * @param description   A human-readable description to include in the metric
     * @param keyValue      additional key/value attributes of the metric (must come in pairs)
     */
    public MetricName metricName(string name, string group, string description, string... keyValue) {
        return metricName(name, group, description, getTags(keyValue));
    }

    /**
     * Create a MetricName with the given name, group and tags, plus default tags specified in the metric
     * configuration. Tag in tags takes precedence if the same tag key is specified in the default metric configuration.
     *
     * @param name  The name of the metric
     * @param group logical group name of the metrics to which this metric belongs
     * @param tags  key/value attributes of the metric
     */
    public MetricName metricName(string name, string group, Dictionary<string, string> tags) {
        return metricName(name, group, "", tags);
    }

    private static Dictionary<string, string> getTags(string... keyValue) {
        if ((keyValue.length % 2) != 0)
            throw new IllegalArgumentException("keyValue needs to be specified in pairs");
        Dictionary<string, string> tags = new LinkedHashMap<string, string>();

        for (int i = 0; i < keyValue.length; i += 2)
            tags.put(keyValue[i], keyValue[i + 1]);
        return tags;
    }

    /**
     * Use the specified domain and metric name templates to generate an HTML table documenting the metrics. A separate table section
     * will be generated for each of the MBeans and the associated attributes. The MBean names are lexicographically sorted to
     * determine the order of these sections. This order is therefore dependent upon the order of the
     * tags in each {@link MetricNameTemplate}.
     *
     * @param domain the domain or prefix for the JMX MBean names; may not be null
     * @param allMetrics the collection of all {@link MetricNameTemplate} instances each describing one metric; may not be null
     * @return the string containing the HTML table; never null
     */
    public static string toHtmlTable(string domain, Iterable<MetricNameTemplate> allMetrics) {
        Dictionary<string, Dictionary<string, string>> beansAndAttributes = new TreeMap<string, Dictionary<string, string>>();

        try (Metrics metrics = new Metrics()) {
            for (MetricNameTemplate template : allMetrics) {
                Dictionary<string, string> tags = new LinkedHashMap<>();
                for (string s : template.tags()) {
                    tags.put(s, "{" + s + "}");
                }

                MetricName metricName = metrics.metricName(template.name(), template.group(), template.description(), tags);
                string mBeanName = JmxReporter.getMBeanName(domain, metricName);
                if (!beansAndAttributes.containsKey(mBeanName)) {
                    beansAndAttributes.put(mBeanName, new TreeMap<string, string>());
                }
                Dictionary<string, string> attrAndDesc = beansAndAttributes.get(mBeanName);
                if (!attrAndDesc.containsKey(template.name())) {
                    attrAndDesc.put(template.name(), template.description());
                } else {
                    throw new IllegalArgumentException("mBean '" + mBeanName + "' attribute '" + template.name() + "' is defined twice.");
                }
            }
        }

        StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");

        for (Entry<string, Dictionary<string, string>> e : beansAndAttributes.entrySet()) {
            b.append("<tr>\n");
            b.append("<td colspan=3 class=\"mbeanName\" style=\"background-color:#ccc; font-weight: bold;\">");
            b.append(e.getKey());
            b.append("</td>");
            b.append("</tr>\n");

            b.append("<tr>\n");
            b.append("<th style=\"width: 90px\"></th>\n");
            b.append("<th>Attribute name</th>\n");
            b.append("<th>Description</th>\n");
            b.append("</tr>\n");

            for (Entry<string, string> e2 : e.getValue().entrySet()) {
                b.append("<tr>\n");
                b.append("<td></td>");
                b.append("<td>");
                b.append(e2.getKey());
                b.append("</td>");
                b.append("<td>");
                b.append(e2.getValue());
                b.append("</td>");
                b.append("</tr>\n");
            }

        }
        b.append("</tbody></table>");

        return b.toString();

    }

    public MetricConfig config() {
        return config;
    }

    /**
     * Get the sensor with the given name if it exists
     * @param name The name of the sensor
     * @return Return the sensor or null if no such sensor exists
     */
    public Sensor getSensor(string name) {
        return this.sensors.get(Utils.notNull(name));
    }

    /**
     * Get or create a sensor with the given unique name and no parent sensors. This uses
     * a default recording level of INFO.
     * @param name The sensor name
     * @return The sensor
     */
    public Sensor sensor(string name) {
        return this.sensor(name, Sensor.RecordingLevel.INFO);
    }

    /**
     * Get or create a sensor with the given unique name and no parent sensors and with a given
     * recording level.
     * @param name The sensor name.
     * @param recordingLevel The recording level.
     * @return The sensor
     */
    public Sensor sensor(string name, Sensor.RecordingLevel recordingLevel) {
        return sensor(name, null, recordingLevel, (Sensor[]) null);
    }


    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor. This uses a default recording level of INFO.
     * @param name The name of the sensor
     * @param parents The parent sensors
     * @return The sensor that is created
     */
    public Sensor sensor(string name, Sensor... parents) {
        return this.sensor(name, Sensor.RecordingLevel.INFO, parents);
    }

    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor.
     * @param name The name of the sensor.
     * @param parents The parent sensors.
     * @param recordingLevel The recording level.
     * @return The sensor that is created
     */
    public Sensor sensor(string name, Sensor.RecordingLevel recordingLevel, Sensor... parents) {
        return sensor(name, null, recordingLevel, parents);
    }

    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor. This uses a default recording level of INFO.
     * @param name The name of the sensor
     * @param config A default configuration to use for this sensor for metrics that don't have their own config
     * @param parents The parent sensors
     * @return The sensor that is created
     */
    public synchronized Sensor sensor(string name, MetricConfig config, Sensor... parents) {
        return this.sensor(name, config, Sensor.RecordingLevel.INFO, parents);
    }


    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor.
     * @param name The name of the sensor
     * @param config A default configuration to use for this sensor for metrics that don't have their own config
     * @param recordingLevel The recording level.
     * @param parents The parent sensors
     * @return The sensor that is created
     */
    public synchronized Sensor sensor(string name, MetricConfig config, Sensor.RecordingLevel recordingLevel, Sensor... parents) {
        return sensor(name, config, Long.MAX_VALUE, recordingLevel, parents);
    }

    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor.
     * @param name The name of the sensor
     * @param config A default configuration to use for this sensor for metrics that don't have their own config
     * @param inactiveSensorExpirationTimeSeconds If no value if recorded on the Sensor for this duration of time,
     *                                        it is eligible for removal
     * @param parents The parent sensors
     * @param recordingLevel The recording level.
     * @return The sensor that is created
     */
    public synchronized Sensor sensor(string name, MetricConfig config, long inactiveSensorExpirationTimeSeconds, Sensor.RecordingLevel recordingLevel, Sensor... parents) {
        Sensor s = getSensor(name);
        if (s == null) {
            s = new Sensor(this, name, parents, config == null ? this.config : config, time, inactiveSensorExpirationTimeSeconds, recordingLevel);
            this.sensors.put(name, s);
            if (parents != null) {
                for (Sensor parent : parents) {
                    List<Sensor> children = childrenSensors.get(parent);
                    if (children == null) {
                        children = new ArrayList<>();
                        childrenSensors.put(parent, children);
                    }
                    children.add(s);
                }
            }
            log.debug("Added sensor with name {}", name);
        }
        return s;
    }

    /**
     * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
     * receive every value recorded with this sensor. This uses a default recording level of INFO.
     * @param name The name of the sensor
     * @param config A default configuration to use for this sensor for metrics that don't have their own config
     * @param inactiveSensorExpirationTimeSeconds If no value if recorded on the Sensor for this duration of time,
     *                                        it is eligible for removal
     * @param parents The parent sensors
     * @return The sensor that is created
     */
    public synchronized Sensor sensor(string name, MetricConfig config, long inactiveSensorExpirationTimeSeconds, Sensor... parents) {
        return this.sensor(name, config, inactiveSensorExpirationTimeSeconds, Sensor.RecordingLevel.INFO, parents);
    }

    /**
     * Remove a sensor (if it exists), associated metrics and its children.
     *
     * @param name The name of the sensor to be removed
     */
    public void removeSensor(string name) {
        Sensor sensor = sensors.get(name);
        if (sensor != null) {
            List<Sensor> childSensors = null;
            synchronized (sensor) {
                synchronized (this) {
                    if (sensors.remove(name, sensor)) {
                        for (KafkaMetric metric : sensor.metrics())
                            removeMetric(metric.metricName());
                        log.debug("Removed sensor with name {}", name);
                        childSensors = childrenSensors.remove(sensor);
                        for (Sensor parent : sensor.parents()) {
                            childrenSensors.getOrDefault(parent, emptyList()).remove(sensor);
                        }
                    }
                }
            }
            if (childSensors != null) {
                for (Sensor childSensor : childSensors)
                    removeSensor(childSensor.name());
            }
        }
    }

    /**
     * Add a metric to monitor an object that implements measurable. This metric won't be associated with any sensor.
     * This is a way to expose existing values as metrics.
     *
     * This method is kept for binary compatibility purposes, it has the same behaviour as
     * {@link #addMetric(MetricName, MetricValueProvider)}.
     *
     * @param metricName The name of the metric
     * @param measurable The measurable that will be measured by this metric
     */
    public void addMetric(MetricName metricName, Measurable measurable) {
        addMetric(metricName, null, measurable);
    }

    /**
     * Add a metric to monitor an object that implements Measurable. This metric won't be associated with any sensor.
     * This is a way to expose existing values as metrics.
     *
     * This method is kept for binary compatibility purposes, it has the same behaviour as
     * {@link #addMetric(MetricName, MetricConfig, MetricValueProvider)}.
     *
     * @param metricName The name of the metric
     * @param config The configuration to use when measuring this measurable
     * @param measurable The measurable that will be measured by this metric
     */
    public void addMetric(MetricName metricName, MetricConfig config, Measurable measurable) {
        addMetric(metricName, config, (MetricValueProvider<?>) measurable);
    }

    /**
     * Add a metric to monitor an object that implements MetricValueProvider. This metric won't be associated with any
     * sensor. This is a way to expose existing values as metrics. User is expected to add any additional
     * synchronization to update and access metric values, if required.
     *
     * @param metricName The name of the metric
     * @param metricValueProvider The metric value provider associated with this metric
     */
    public void addMetric(MetricName metricName, MetricConfig config, MetricValueProvider<?> metricValueProvider) {
        KafkaMetric m = new KafkaMetric(new Object(),
                                        Utils.notNull(metricName),
                                        Utils.notNull(metricValueProvider),
                                        config == null ? this.config : config,
                                        time);
        registerMetric(m);
    }

    /**
     * Add a metric to monitor an object that implements MetricValueProvider. This metric won't be associated with any
     * sensor. This is a way to expose existing values as metrics. User is expected to add any additional
     * synchronization to update and access metric values, if required.
     *
     * @param metricName The name of the metric
     * @param metricValueProvider The metric value provider associated with this metric
     */
    public void addMetric(MetricName metricName, MetricValueProvider<?> metricValueProvider) {
        addMetric(metricName, null, metricValueProvider);
    }

    /**
     * Remove a metric if it exists and return it. Return null otherwise. If a metric is removed, `metricRemoval`
     * will be invoked for each reporter.
     *
     * @param metricName The name of the metric
     * @return the removed `KafkaMetric` or null if no such metric exists
     */
    public synchronized KafkaMetric removeMetric(MetricName metricName) {
        KafkaMetric metric = this.metrics.remove(metricName);
        if (metric != null) {
            for (MetricsReporter reporter : reporters) {
                try {
                    reporter.metricRemoval(metric);
                } catch (Exception e) {
                    log.error("Error when removing metric from " + reporter.GetType().getName(), e);
                }
            }
            log.trace("Removed metric named {}", metricName);
        }
        return metric;
    }

    /**
     * Add a MetricReporter
     */
    public synchronized void addReporter(MetricsReporter reporter) {
        Utils.notNull(reporter).init(new ArrayList<>(metrics.values()));
        this.reporters.add(reporter);
    }

/**
 * Remove a MetricReporter
 */
[MethodImpl(MethodImplOptions.Synchronized)]
public void removeReporter(MetricsReporter reporter) {
        if (this.reporters.remove(reporter)) {
            reporter.close();
        }
    }

[MethodImpl(MethodImplOptions.Synchronized)]
void registerMetric(KafkaMetric metric) {
        MetricName metricName = metric.metricName();
        if (this.metrics.containsKey(metricName))
            throw new IllegalArgumentException("A metric named '" + metricName + "' already exists, can't register another one.");
        this.metrics.put(metricName, metric);
        for (MetricsReporter reporter : reporters) {
            try {
                reporter.metricChange(metric);
            } catch (Exception e) {
                log.error("Error when registering metric on " + reporter.GetType().getName(), e);
            }
        }
        log.trace("Registered metric named {}", metricName);
    }

    /**
     * Get all the metrics currently maintained indexed by metricName
     */
    public Dictionary<MetricName, KafkaMetric> metrics() {
        return this.metrics;
    }

    public List<MetricsReporter> reporters() {
        return this.reporters;
    }

    public KafkaMetric metric(MetricName metricName) {
        return this.metrics.get(metricName);
    }

    /**
     * This iterates over every Sensor and triggers a removeSensor if it has expired
     * Package private for testing
     */
    public class ExpireSensorTask : Runnable
{
        public void run() {
            foreach (KeyValuePair<string, Sensor> sensorEntry in sensors.entrySet()) {
                // removeSensor also locks the sensor object. This is fine because synchronized is reentrant
                // There is however a minor race condition here. Assume we have a parent sensor P and child sensor C.
                // Calling record on C would cause a record on P as well.
                // So expiration time for P == expiration time for C. If the record on P happens via C just after P is removed,
                // that will cause C to also get removed.
                // Since the expiration time is typically high it is not expected to be a significant concern
                // and thus not necessary to optimize
                synchronized (sensorEntry.getValue()) {
                    if (sensorEntry.getValue().hasExpired()) {
                        log.debug("Removing expired sensor {}", sensorEntry.getKey());
                        removeSensor(sensorEntry.getKey());
                    }
                }
            }
        }
    }

    /* For testing use only. */
    IReadOnlyDictionary<Sensor, List<Sensor>> childrenSensors() {
        return new IReadOnlyDictionary(childrenSensors);
    }

    public MetricName metricInstance(MetricNameTemplate template, string[] keyValue) {
        return metricInstance(template, getTags(keyValue));
    }

    public MetricName metricInstance(MetricNameTemplate template, Dictionary<string, string> tags) {
        // check to make sure that the runtime defined tags contain all the template tags.
        Set<string> runtimeTagKeys = new HashSet<string>(tags.keySet());
        runtimeTagKeys.addAll(config().tags().keySet());

        Set<string> templateTagKeys = template.tags();

        if (!runtimeTagKeys.Equals(templateTagKeys)) {
            throw new ArgumentException("For '" + template.name() + "', runtime-defined metric tags do not match the tags in the template. "
                    + "Runtime = " + runtimeTagKeys.toString() + " Template = " + templateTagKeys.toString());
        }

        return this.metricName(template.name(), template.group(), template.description(), tags);
    }

    /**
     * Close this metrics repository.
     */
    @Override
    public void close() {
        if (this.metricsScheduler != null) {
            this.metricsScheduler.shutdown();
            try {
                this.metricsScheduler.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                // ignore and continue shutdown
                Thread.CurrentThread.Interrupt();
            }
        }

        foreach (MetricsReporter reporter in reporters) {
            try {
                reporter.close();
            } catch (Exception e) {
                log.error("Error when closing " + reporter.GetType().getName(), e);
            }
        }
    }

}
}