using Kafka.Common.Metrics.Interfaces;
using Kafka.Common.Utils;
using Kafka.Common.Utils.Interfaces;
using Kakfa.Common.Metrics;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

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
     * sensor.Add(metricName, new Avg());
     * metricName = new MetricName(&quot;message-size-max&quot;, &quot;producer-metrics&quot;);
     * sensor.Add(metricName, new Max());
     *
     * // as messages are sent we record the sizes
     * sensor.record(messageSize);
     * </pre>
     */
    public class MetricsRegistry : IDisposable
    {
        //private MetricConfig config;
        //private ConcurrentDictionary<MetricName, KafkaMetric> metrics;
        //private ConcurrentDictionary<Sensor, List<Sensor>> childrenSensors;
        //private List<IMetricsReporter> reporters;
        private ITime time;
        private ScheduledThreadPoolExecutor metricsScheduler;
        private static ILogger log = new LoggerFactory().CreateLogger<MetricsRegistry>();

        /**
         * Create a metrics repository with no metric reporters and default configuration.
         * Expiration of Sensors is disabled.
         */
        public MetricsRegistry()
            : this(new MetricConfig())
        {
        }

        /**
         * Create a metrics repository with no metric reporters and default configuration.
         * Expiration of Sensors is disabled.
         */
        public MetricsRegistry(ITime time)
            : this(new MetricConfig(), new List<IMetricsReporter>(0), time)
        {
        }

        /**
         * Create a metrics repository with no metric reporters and the given default configuration.
         * Expiration of Sensors is disabled.
         */
        public MetricsRegistry(MetricConfig defaultConfig, ITime time)
            : this(defaultConfig, new List<IMetricsReporter>(0), time)
        {
        }

        /**
           * Create a metrics repository with no reporters and the given default config. This config will be used for any
           * metric that doesn't override its own config. Expiration of Sensors is disabled.
           * @param defaultConfig The default config to use for all metrics that don't override their config
           */
        public MetricsRegistry(MetricConfig defaultConfig)
            : this(defaultConfig, new List<IMetricsReporter>(0), Time.SYSTEM)
        {
        }

        /**
         * Create a metrics repository with a default config and the given metric reporters.
         * Expiration of Sensors is disabled.
         * @param defaultConfig The default config
         * @param reporters The metrics reporters
         * @param time The time instance to use with the metrics
         */
        public MetricsRegistry(MetricConfig defaultConfig, List<IMetricsReporter> reporters, ITime time)
            : this(defaultConfig, reporters, time, false)
        {
        }

        /**
         * Create a metrics repository with a default config, given metric reporters and the ability to expire eligible sensors
         * @param defaultConfig The default config
         * @param reporters The metrics reporters
         * @param time The time instance to use with the metrics
         * @param enableExpiration true if the metrics instance can garbage collect inactive sensors, false otherwise
         */
        public MetricsRegistry(MetricConfig defaultConfig,
            List<IMetricsReporter> reporters,
            ITime time,
            bool enableExpiration)
        {
            this.config = defaultConfig;
            this.metrics = new ConcurrentDictionary<MetricName, KafkaMetric>();
            this.reporters = reporters ?? throw new ArgumentNullException(nameof(reporters));

            this.time = time;
            foreach (IMetricsReporter reporter in reporters)
            {
                reporter.init(new List<KafkaMetric>());
            }

            // Create the ThreadPoolExecutor only if expiration of Sensors is enabled.
            if (enableExpiration)
            {
                this.metricsScheduler = new ScheduledThreadPoolExecutor(1);
                // Creating a daemon thread to not block shutdown
                //    this.metricsScheduler.setThreadFactory(new ThreadFactory()
                //{

                //        public Thread newThread(Runnable runnable)
                //  {
                //        return KafkaThread.daemon("SensorExpiryThread", runnable);
                //    }
                //});

                //this.metricsScheduler.scheduleAtFixedRate(new ExpireSensorTask(), 30, 30, TimeUnit.SECONDS);
            }
            else
            {
                this.metricsScheduler = null;
            }
        }

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

        // This code.Added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }

        //void addMetric(metricName("count", "kafka-metrics-count", "total number of registered metrics"),
        //    new Measurable()
        //    {
        //        public double measure(MetricConfig config, long now)
        //        {
        //            return metrics.size();
        //        }
        //    });

        /**
         * Create a MetricName with the given name, group, description and tags, plus default tags specified in the metric
         * configuration. Tag in tags takes precedence if the same tag key is specified in the default metric configuration.
         *
         * @param name        The name of the metric
         * @param group       logical group name of the metrics to which this metric belongs
         * @param description A human-readable description to include in the metric
         * @param tags       .Additional key/value attributes of the metric
         */
        public MetricName metricName(string name, string group, string description, Dictionary<string, string> tags)
        {
            Dictionary<string, string> combinedTag = new Dictionary<string, string>(config.tags);
            //    combinedTag.AddRange(tags);

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
        public MetricName metricName(string name, string group, string description)
        {
            return metricName(name, group, description, new Dictionary<string, string>());
        }

        /**
         * Create a MetricName with the given name, group and default tags specified in the metric configuration.
         *
         * @param name        The name of the metric
         * @param group       logical group name of the metrics to which this metric belongs
         */
        public MetricName metricName(string name, string group)
        {
            return metricName(name, group, "", new Dictionary<string, string>());
        }

        /**
         * Create a MetricName with the given name, group, description, and keyValue as tags,  plus default tags specified in the metric
         * configuration. Tag in keyValue takes precedence if the same tag key is specified in the default metric configuration.
         *
         * @param name          The name of the metric
         * @param group         logical group name of the metrics to which this metric belongs
         * @param description   A human-readable description to include in the metric
         * @param keyValue     .Additional key/value attributes of the metric (must come in pairs)
         */
        public MetricName metricName(string name, string group, string description, string[] keyValue)
        {
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
        public MetricName metricName(string name, string group, Dictionary<string, string> tags)
        {
            return metricName(name, group, "", tags);
        }

        private static Dictionary<string, string> getTags(string[] keyValue)
        {
            if ((keyValue.Length % 2) != 0)
            {
                throw new System.ArgumentException("keyValue needs to be specified in pairs");
            }

            Dictionary<string, string> tags = new Dictionary<string, string>();

            for (int i = 0; i < keyValue.Length; i += 2)
            {
                tags.Add(keyValue[i], keyValue[i + 1]);
            }

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
        public static string toHtmlTable(string domain, IEnumerable<MetricNameTemplate> allMetrics)
        {
            Dictionary<string, Dictionary<string, string>> beansAndAttributes = new Dictionary<string, Dictionary<string, string>>();

            using (MetricsRegistry metrics = new MetricsRegistry())
            {
                foreach (MetricNameTemplate template in allMetrics)
                {
                    Dictionary<string, string> tags = new Dictionary<string, string>();
                    foreach (string s in template.tags)
                    {
                        tags.Add(s, "{" + s + "}");
                    }

                    MetricName metricName = metrics.metricName(template.name, template.group, template.description, tags);
                    //string mBeanName = JmxReporter.getMBeanName(domain, metricName);
                    //if (!beansAndAttributes.ContainsKey(mBeanName))
                    {
                        //    beansAndAttributes.Add(mBeanName, new TreeMap<string, string>());
                        //}
                        //Dictionary<string, string> attrAndDesc = beansAndAttributes["mBeanName"];
                        //if (!attrAndDesc.ContainsKey(template.name()))
                        //{
                        //    attrAndDesc.Add(template.name(), template.description());
                        //}
                        //else
                        //{
                        //    throw new System.ArgumentException("mBean '" + "mBeanName" + "' attribute '" + template.name() + "' is defined twice.");
                        //}
                    }
                }

                StringBuilder b = new StringBuilder();
                //b.Append("<table=\"data-table\"><tbody>\n");

                //foreach (Entry<string, Dictionary<string, string>> e in beansAndAttributes.entrySet())
                //{
                //    //    b.Append("<tr>\n");
                //    //    b.Append("<td colspan=3=\"mbeanName\" style=\"background-color:#ccc; font-weight: bold;\">");
                //    //    b.Append(e.Key);
                //    //    b.Append("</td>");
                //    //    b.Append("</tr>\n");

                //    //    b.Append("<tr>\n");
                //    //    b.Append("<th style=\"width: 90px\"></th>\n");
                //    //    b.Append("<th>Attribute name</th>\n");
                //    //    b.Append("<th>Description</th>\n");
                //    //    b.Append("</tr>\n");

                //    //    foreach (Entry<string, string> e2 in e.Value.entrySet())
                //    {
                //        //        b.Append("<tr>\n");
                //        //        b.Append("<td></td>");
                //        //        b.Append("<td>");
                //        //        b.Append(e2.Key);
                //        //        b.Append("</td>");
                //        //        b.Append("<td>");
                //        //        b.Append(e2.Value);
                //        //        b.Append("</td>");
                //        //        b.Append("</tr>\n");
                //        //    }

                //        //}
                //        //b.Append("</tbody></table>");

                //        return b.ToString();
                //    }
            }

            return "";
        }

        public MetricConfig config { get; }

        /**
         * Get the sensor with the given name if it exists
         * @param name The name of the sensor
         * @return Return the sensor or null if no such sensor exists
         */
        public Sensor getSensor(string name)
        {
            return this.sensors[name];
        }

        /**
         * Get or create a sensor with the given unique name and no parent sensors. This uses
         * a default recording level of INFO.
         * @param name The sensor name
         * @return The sensor
         */
        public Sensor sensor(string name)
        {
            return this.sensor(name, RecordingLevel.INFO);
        }

        /**
         * Get or create a sensor with the given unique name and no parent sensors and with a given
         * recording level.
         * @param name The sensor name.
         * @param recordingLevel The recording level.
         * @return The sensor
         */
        public Sensor sensor(string name, RecordingLevel recordingLevel)
        {
            return sensor(name, null, recordingLevel, new List<Sensor>());
        }

        /**
         * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
         * receive every value recorded with this sensor. This uses a default recording level of INFO.
         * @param name The name of the sensor
         * @param parents The parent sensors
         * @return The sensor that is created
         */
        public Sensor sensor(string name, List<Sensor> parents)
        {
            return this.sensor(name, RecordingLevel.INFO, parents);
        }

        /**
         * Get or create a sensor with the given unique name and zero or more parent sensors. All parent sensors will
         * receive every value recorded with this sensor.
         * @param name The name of the sensor.
         * @param parents The parent sensors.
         * @param recordingLevel The recording level.
         * @return The sensor that is created
         */
        public Sensor sensor(string name, RecordingLevel recordingLevel, List<Sensor> parents)
        {
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
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Sensor sensor(string name, MetricConfig config, List<Sensor> parents)
        {
            return this.sensor(name, config, RecordingLevel.INFO, parents);
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
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Sensor sensor(string name, MetricConfig config, RecordingLevel recordingLevel, List<Sensor> parents)
        {
            return sensor(name, config, long.MaxValue, recordingLevel, parents);
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
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Sensor sensor(string name, MetricConfig config, long inactiveSensorExpirationTimeSeconds, RecordingLevel recordingLevel, List<Sensor> parents)
        {
            Sensor s = getSensor(name);
            if (s == null)
            {
                s = new Sensor(this, name, parents, config == null ? this.config : config, time, inactiveSensorExpirationTimeSeconds, recordingLevel);
                this.sensors.TryAdd(name, s);
                if (parents != null)
                {
                    foreach (Sensor parent in parents)
                    {
                        List<Sensor> children = childrenSensors[parent];
                        if (children == null)
                        {
                            children = new List<Sensor>();
                            childrenSensors.TryAdd(parent, children);
                        }

                        children.Add(s);
                    }
                }

                log.LogDebug("Added sensor with name {}", name);
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
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Sensor sensor(string name, MetricConfig config, long inactiveSensorExpirationTimeSeconds, List<Sensor> parents)
        {
            return this.sensor(name, config, inactiveSensorExpirationTimeSeconds, RecordingLevel.INFO, parents);
        }

        /**
         * Remove a sensor (if it exists), associated metrics and its children.
         *
         * @param name The name of the sensor to be removed
         */
        public void removeSensor(string name)
        {
            Sensor sensor = sensors[name];
            if (sensor != null)
            {
                List<Sensor> childSensors = null;
                lock (sensor)
                {
                    lock (this)
                    {
                        if (sensors.TryRemove(name, out var removedSensor))
                        {
                            foreach (KafkaMetric metric in removedSensor.metrics.Values)
                            {
                                removeMetric(metric.metricName);
                            }

                            log.LogDebug("Removed sensor with name {}", name);
                            childrenSensors.TryRemove(sensor, out var _);

                            foreach (Sensor parent in removedSensor.parents)
                            {
                                if (childrenSensors.TryGetValue(parent, out var sensors))
                                {
                                    sensors.Remove(sensor);
                                }
                            }
                        }
                    }
                }
                if (childSensors != null)
                {
                    foreach (Sensor childSensor in childSensors)
                    {
                        removeSensor(childSensor.name);
                    }
                }
            }
        }

        /**
         * Add a metric to monitor an object that : measurable. This metric won't be associated with any sensor.
         * This is a way to expose existing values as metrics.
         *
         * This method is kept for binary compatibility purposes, it has the same behaviour as
         * {@link .AddMetric(MetricName, MetricValueProvider)}.
         *
         * @param metricName The name of the metric
         * @param measurable The measurable that will be measured by this metric
         */
        public void addMetric(MetricName metricName, IMeasurable measurable)
        {
            addMetric(metricName, null, measurable);
        }

        /**
         * Add a metric to monitor an object that : Measurable. This metric won't be associated with any sensor.
         * This is a way to expose existing values as metrics.
         *
         * This method is kept for binary compatibility purposes, it has the same behaviour as
         * {@link .AddMetric(MetricName, MetricConfig, MetricValueProvider)}.
         *
         * @param metricName The name of the metric
         * @param config The configuration to use when measuring this measurable
         * @param measurable The measurable that will be measured by this metric
         */
        public void addMetric(MetricName metricName, MetricConfig config, IMeasurable measurable)
        {
            addMetric(metricName, config, (IMetricValueProvider)measurable);
        }

        /**
         * Add a metric to monitor an object that : MetricValueProvider. This metric won't be associated with any
         * sensor. This is a way to expose existing values as metrics. User is expected to.Add any.Additional
         * synchronization to update and access metric values, if required.
         *
         * @param metricName The name of the metric
         * @param metricValueProvider The metric value provider associated with this metric
         */
        public void addMetric(MetricName metricName, MetricConfig config, IMetricValueProvider metricValueProvider)
        {
            var m = new KafkaMetric(
                new object(),
                metricName ?? throw new ArgumentNullException(nameof(metricName)),
                metricValueProvider ?? throw new ArgumentNullException(nameof(metricValueProvider)),
                config,// == null ? this.config : config,
                time);

            registerMetric(m);
        }

        /**
         * Add a metric to monitor an object that : MetricValueProvider. This metric won't be associated with any
         * sensor. This is a way to expose existing values as metrics. User is expected to.Add any.Additional
         * synchronization to update and access metric values, if required.
         *
         * @param metricName The name of the metric
         * @param metricValueProvider The metric value provider associated with this metric
         */
        public void addMetric(MetricName metricName, IMetricValueProvider metricValueProvider)
        {
            addMetric(metricName, null, metricValueProvider);
        }

        /**
         * Remove a metric if it exists and return it. Return null otherwise. If a metric is removed, `metricRemoval`
         * will be invoked for each reporter.
         *
         * @param metricName The name of the metric
         * @return the removed `KafkaMetric` or null if no such metric exists
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public KafkaMetric removeMetric(MetricName metricName)
        {
            if (this.metrics.TryRemove(metricName, out var metric))
            {
                if (metric != null)
                {
                    foreach (IMetricsReporter reporter in reporters)
                    {
                        try
                        {
                            reporter.metricRemoval(metric);
                        }
                        catch (Exception e)
                        {
                            log.LogError("Error when removing metric from " + reporter.GetType().Name, e);
                        }
                    }

                    log.LogTrace("Removed metric named {}", metricName);

                    return metric;
                }
            }

            return null;
        }

        /**
         * Add a MetricReporter
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void addReporter(IMetricsReporter reporter)
        {
            reporter.init(new List<KafkaMetric>(metrics.Values));
            this.reporters.Add(reporter);
        }

        /**
         * Remove a MetricReporter
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void removeReporter(IMetricsReporter reporter)
        {
            if (this.reporters.Remove(reporter))
            {
                reporter.close();
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void registerMetric(KafkaMetric metric)
        {
            MetricName metricName = metric.metricName;
            if (this.metrics.ContainsKey(metricName))
            {
                throw new System.ArgumentException("A metric named '" + metricName + "' already exists, can't register another one.");
            }

            this.metrics.TryAdd(metricName, metric);
            foreach (IMetricsReporter reporter in reporters)
            {
                try
                {
                    reporter.metricChange(metric);
                }
                catch (Exception e)
                {
                    log.LogError("Error when registering metric on " + reporter.GetType().Name, e);
                }
            }

            log.LogTrace("Registered metric named {}", metricName);
        }

        /**
         * Get all the metrics currently maintained indexed by metricName
         */
        public ConcurrentDictionary<MetricName, KafkaMetric> metrics { get; } = new ConcurrentDictionary<MetricName, KafkaMetric>();
        public ConcurrentDictionary<string, Sensor> sensors { get; } = new ConcurrentDictionary<string, Sensor>();
        public ConcurrentDictionary<Sensor, List<Sensor>> childrenSensors { get; } = new ConcurrentDictionary<Sensor, List<Sensor>>();

        public List<IMetricsReporter> reporters { get; } = new List<IMetricsReporter>();

        public KafkaMetric metric(MetricName metricName)
        {
            return this.metrics[metricName];
        }

        public MetricName metricInstance(MetricNameTemplate template, string[] keyValue)
        {
            return metricInstance(template, getTags(keyValue));
        }

        public MetricName metricInstance(MetricNameTemplate template, Dictionary<string, string> tags)
        {
            // check to make sure that the runtime defined tags contain all the template tags.
            HashSet<string> runtimeTagKeys = new HashSet<string>(tags.Keys);
            runtimeTagKeys.UnionWith(config.tags.Keys);

            HashSet<string> templateTagKeys = template.tags;

            if (!runtimeTagKeys.Equals(templateTagKeys))
            {
                throw new System.ArgumentException("For '" + template.name + "', runtime-defined metric tags do not match the tags in the template. "
                        + "Runtime = " + runtimeTagKeys.ToString() + " Template = " + templateTagKeys.ToString());
            }

            return this.metricName(template.name, template.group, template.description, tags);
        }

        /**
         * Close this metrics repository.
         */
        public void close()
        {
            if (this.metricsScheduler != null)
            {
                //                this.metricsScheduler.shutdown();
                try
                {

                    //this.metricsScheduler.awaitTermination(30, TimeUnit.SECONDS);
                }
                catch (ThreadInterruptedException ex)
                {
                    // ignore and continue shutdown
                    Thread.CurrentThread.Interrupt();
                }
            }

            foreach (IMetricsReporter reporter in reporters)
            {
                try
                {
                    reporter.close();
                }
                catch (Exception e)
                {
                    log.LogError("Error when closing " + reporter.GetType().Name, e);
                }
            }
        }
    }
}