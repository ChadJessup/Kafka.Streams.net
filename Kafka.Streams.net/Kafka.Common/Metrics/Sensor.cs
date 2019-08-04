using Kafka.Common.Metrics.Stats;
using Kafka.Common.Utils.Interfaces;
using Kakfa.Common.Metrics;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.CompilerServices;
using static Kafka.Common.Metrics.ICompoundStat;

namespace Kafka.Common.Metrics
{
    /**
     * A sensor applies a continuous sequence of numerical values to a set of associated metrics. For example a sensor on
     * message size would record a sequence of message sizes using the {@link #record(double)} api and would maintain a set
     * of metrics about request sizes such as the average or max.
     */
    public class Sensor
    {
        public static RecordingLevel RecordingLevel;

        private MetricsRegistry registry;
        public List<Sensor> parents = new List<Sensor>();
        private List<IStat> stats;
        public Dictionary<MetricName, KafkaMetric> metrics { get; } = new Dictionary<MetricName, KafkaMetric>();
        private MetricConfig config;
        private ITime time;
        private long lastRecordTime;
        private long inactiveSensorExpirationTimeMs;

        public Sensor(
            MetricsRegistry registry,
            string name,
            List<Sensor> parents,
            MetricConfig config,
            ITime time,
            long inactiveSensorExpirationTimeSeconds,
            RecordingLevel recordingLevel)
        {
            this.registry = registry;
            this.name = name ?? throw new ArgumentNullException(nameof(name));
            this.parents = parents ?? new List<Sensor>();
            this.metrics = new Dictionary<MetricName, KafkaMetric>();
            this.stats = new List<IStat>();
            this.config = config;
            this.time = time;
//            this.inactiveSensorExpirationTimeMs = TimeUnit.MILLISECONDS.convert(inactiveSensorExpirationTimeSeconds, TimeUnit.SECONDS);
            this.lastRecordTime = time.milliseconds();
            RecordingLevel = recordingLevel;

            this.metricLock = new object();
            checkForest(new HashSet<Sensor>());
        }

        /* Validate that this sensor doesn't end up referencing itself */
        private void checkForest(HashSet<Sensor> sensors)
        {
            if (!sensors.Add(this))
            {
                throw new ArgumentException("Circular dependency in sensors: " + name + " is its own parent.");
            }

            foreach (Sensor parent in parents)
            {
                parent.checkForest(sensors);
            }
        }

        /**
         * The name this sensor is registered with. This name will be unique among all registered sensors.
         */
        public string name { get; }

        IReadOnlyList<Sensor> Parents() => parents.ToList();

        /**
         * Record an occurrence, this is just short-hand for {@link #record(double) record(1.0)}
         */
        public void record()
        {
            if (shouldRecord())
            {
                record(1.0);
            }
        }

        /**
         * @return true if the sensor's record level indicates that the metric will be recorded, false otherwise
         */
        public bool shouldRecord()
        {
            return RecordingLevel.shouldRecord(config.recordingLevel.id);
        }
        /**
         * Record a value with this sensor
         * @param value The value to record
         * @throws QuotaViolationException if recording this value moves a metric beyond its configured maximum or minimum
         *         bound
         */
        public void record(double value)
        {
            if (shouldRecord())
            {
                record(value, time.milliseconds());
            }
        }

        /**
         * Record a value at a known time. This method is slightly faster than {@link #record(double)} since it will reuse
         * the time stamp.
         * @param value The value we are recording
         * @param timeMs The current POSIX time in milliseconds
         * @throws QuotaViolationException if recording this value moves a metric beyond its configured maximum or minimum
         *         bound
         */
        public void record(double value, long timeMs)
        {
            record(value, timeMs, true);
        }

        public void record(double value, long timeMs, bool shouldCheckQuotas)
        {
            if (shouldRecord())
            {
                this.lastRecordTime = timeMs;
                lock (this)
                {
                    lock (metricLock)
                    {
                        // increment all the stats
                        foreach (IStat stat in this.stats)
                        {
                            stat.record(config, value, timeMs);
                        }
                    }

                    if (shouldCheckQuotas)
                    {
                        checkQuotas(timeMs);
                    }
                }

                foreach (Sensor parent in parents)
                {
                    parent.record(value, timeMs, shouldCheckQuotas);
                }
            }
        }

        /**
         * Check if we have violated our quota for any metric that has a configured quota
         */
        public void checkQuotas()
        {
            checkQuotas(time.milliseconds());
        }

        public void checkQuotas(long timeMs)
        {
            foreach (KafkaMetric metric in this.metrics.Values)
            {
                MetricConfig config = metric.config;
                if (config != null)
                {
                    Quota quota = config.quota;
                    if (quota != null)
                    {
                        double value = metric.measurableValue(timeMs);
                        if (!quota.acceptable(value))
                        {
                            throw new QuotaViolationException(
                                metric.metricName,
                                value,
                                quota.bound);
                        }
                    }
                }
            }
        }

        /**
         * Register a compound statistic with this sensor with no config override
         * @param stat The stat to register
         * @return true if stat is added to sensor, false if sensor is expired
         */
        public bool add(ICompoundStat stat)
        {
            return add(stat, null);
        }

        /**
         * Register a compound statistic with this sensor which yields multiple measurable quantities (like a histogram)
         * @param stat The stat to register
         * @param config The configuration for this stat. If null then the stat will use the default configuration for this
         *        sensor.
         * @return true if stat is added to sensor, false if sensor is expired
         */
        [MethodImpl(MethodImplOptions.Synchronized])
        public bool add(ICompoundStat stat, MetricConfig config)
        {
            if (hasExpired())
                return false;

            stat = stat ?? throw new NullReferenceException();

            this.stats.Add(stat);
            object @lock = metricLock;
            foreach (NamedMeasurable m in stat.stats())
            {
                KafkaMetric metric = new KafkaMetric(@lock, m.name, m.stat, config == null ? this.config : config, time);
                if (!metrics.ContainsKey(metric.metricName))
                {
                    registry.registerMetric(metric);
                    metrics.Add(metric.metricName, metric);
                }
            }

            return true;
        }

        /**
         * Register a metric with this sensor
         * @param metricName The name of the metric
         * @param stat The statistic to keep
         * @return true if metric is added to sensor, false if sensor is expired
         */
        public bool add(MetricName metricName, IMeasurableStat stat)
        {
            return add(metricName, stat, null);
        }

        /**
         * Register a metric with this sensor
         *
         * @param metricName The name of the metric
         * @param stat       The statistic to keep
         * @param config     A special configuration for this metric. If null use the sensor default configuration.
         * @return true if metric is added to sensor, false if sensor is expired
         */
        [MethodImpl(MethodImplOptions.Synchronized])
        public bool add(
            MetricName metricName,
            IMeasurableStat stat,
            MetricConfig config)
        {
            if (hasExpired())
            {
                return false;
            }
            else if (metrics.ContainsKey(metricName))
            {
                return true;
            }
            else
            {
                KafkaMetric metric = new KafkaMetric(
                    this.metricLock,
                    metricName ?? throw new ArgumentNullException(nameof(metricName)),
                    stat ?? throw new ArgumentNullException(nameof(stat)),
                    config == null ? this.config : config,
                    time);

                registry.registerMetric(metric);
                metrics.Add(metric.metricName, metric);
                stats.Add(stat);
                return true;
            }
        }

        /**
         * Return true if the Sensor is eligible for removal due to inactivity.
         *        false otherwise
         */
        public bool hasExpired()
        {
            return (time.milliseconds() - this.lastRecordTime) > this.inactiveSensorExpirationTimeMs;
        }

        [MethodImpl(MethodImplOptions.Synchronized])
        IReadOnlyList<KafkaMetric> GetMetrics()
        {
            return this.metrics.Values
                .ToList()
                .AsReadOnly();
        }

        /**
         * KafkaMetrics of sensors which use SampledStat should be synchronized on the same lock
         * for sensor record and metric value read to allow concurrent reads and updates. For simplicity,
         * all sensors are synchronized on this object.
         * <p>
         * Sensor object is not used as a lock for reading metric value since metrics reporter is
         * invoked while holding Sensor and Metrics locks to report addition and removal of metrics
         * and synchronized reporters may deadlock if Sensor lock is used for reading metrics values.
         * Note that Sensor object itself is used as a lock to protect the access to stats and metrics
         * while recording metric values, adding and deleting sensors.
         * </p><p>
         * Locking order (assume all MetricsReporter methods may be synchronized):
         * <ul>
         *   <li>Sensor#add: Sensor -> Metrics -> MetricsReporter</li>
         *   <li>Metrics#removeSensor: Sensor -> Metrics -> MetricsReporter</li>
         *   <li>KafkaMetric#metricValue: MetricsReporter -> Sensor#metricLock</li>
         *   <li>Sensor#record: Sensor -> Sensor#metricLock</li>
         * </ul>
         * </p>
         */
        private object metricLock { get; }
    }
}