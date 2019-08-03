using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Common.Metrics
{
    /**
     * A sensor applies a continuous sequence of numerical values to a set of associated metrics. For example a sensor on
     * message size would record a sequence of message sizes using the {@link #record(double)} api and would maintain a set
     * of metrics about request sizes such as the average or max.
     */
    public class Sensor
    {

            private Metrics registry;
    private string name;
    private Sensor[] parents;
    private List<Stat> stats;
    private Dictionary<MetricName, KafkaMetric> metrics;
    private MetricConfig config;
    private Time time;
    private volatile long lastRecordTime;
            private long inactiveSensorExpirationTimeMs;
            private object metricLock;

    public enum RecordingLevel
            {
                INFO(0, "INFO"), DEBUG(1, "DEBUG");

            private static RecordingLevel[] ID_TO_TYPE;
        private static int MIN_RECORDING_LEVEL_KEY = 0;
            public static int MAX_RECORDING_LEVEL_KEY;

            static {
            int maxRL = -1;
            for (RecordingLevel level : RecordingLevel.values()) {
                maxRL = Math.max(maxRL, level.id);
            }
        RecordingLevel[] idToName = new RecordingLevel[maxRL + 1];
            for (RecordingLevel level : RecordingLevel.values()) {
                idToName[level.id] = level;
            }
    ID_TO_TYPE = idToName;
            MAX_RECORDING_LEVEL_KEY = maxRL;
        }

/** an english description of the api--this is for debugging and can change */
public string name;

        /** the permanent and immutable id of an API--this can't change ever */
        public short id;

RecordingLevel(int id, string name)
{
    this.id = (short)id;
    this.name = name;
}

public static RecordingLevel forId(int id)
{
    if (id < MIN_RECORDING_LEVEL_KEY || id > MAX_RECORDING_LEVEL_KEY)
        throw new IllegalArgumentException(string.format("Unexpected RecordLevel id `%d`, it should be between `%d` " +
            "and `%d` (inclusive)", id, MIN_RECORDING_LEVEL_KEY, MAX_RECORDING_LEVEL_KEY));
    return ID_TO_TYPE[id];
}

/** Case insensitive lookup by protocol name */
public static RecordingLevel forName(string name)
{
    return RecordingLevel.valueOf(name.toUpperCase(Locale.ROOT));
}

public bool shouldRecord(int configId)
{
    return configId == DEBUG.id || configId == this.id;
}

    }

    private RecordingLevel recordingLevel;

    Sensor(Metrics registry, string name, Sensor[] parents, MetricConfig config, Time time,
           long inactiveSensorExpirationTimeSeconds, RecordingLevel recordingLevel)
{
    super();
    this.registry = registry;
    this.name = Utils.notNull(name);
    this.parents = parents == null ? new Sensor[0] : parents;
    this.metrics = new LinkedHashMap<>();
    this.stats = new ArrayList<>();
    this.config = config;
    this.time = time;
    this.inactiveSensorExpirationTimeMs = TimeUnit.MILLISECONDS.convert(inactiveSensorExpirationTimeSeconds, TimeUnit.SECONDS);
    this.lastRecordTime = time.milliseconds();
    this.recordingLevel = recordingLevel;
    this.metricLock = new Object();
    checkForest(new HashSet<Sensor>());
}

/* Validate that this sensor doesn't end up referencing itself */
private void checkForest(Set<Sensor> sensors)
{
    if (!sensors.add(this))
        throw new IllegalArgumentException("Circular dependency in sensors: " + name() + " is its own parent.");
    for (Sensor parent : parents)
        parent.checkForest(sensors);
}

/**
 * The name this sensor is registered with. This name will be unique among all registered sensors.
 */
public string name()
{
    return this.name;
}

List<Sensor> parents()
{
    return unmodifiableList(asList(parents));
}

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
    return this.recordingLevel.shouldRecord(config.recordLevel().id);
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

public void record(double value, long timeMs, bool checkQuotas)
{
    if (shouldRecord())
    {
        this.lastRecordTime = timeMs;
        synchronized(this) {
            synchronized(metricLock()) {
                // increment all the stats
                for (Stat stat : this.stats)
                    stat.record(config, value, timeMs);
            }
            if (checkQuotas)
                checkQuotas(timeMs);
        }
        for (Sensor parent : parents)
            parent.record(value, timeMs, checkQuotas);
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
    for (KafkaMetric metric : this.metrics.values())
    {
        MetricConfig config = metric.config();
        if (config != null)
        {
            Quota quota = config.quota();
            if (quota != null)
            {
                double value = metric.measurableValue(timeMs);
                if (!quota.acceptable(value))
                {
                    throw new QuotaViolationException(metric.metricName(), value,
                        quota.bound());
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
public bool add(CompoundStat stat)
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
public synchronized bool add(CompoundStat stat, MetricConfig config)
{
    if (hasExpired())
        return false;

    this.stats.add(Utils.notNull(stat));
    object lock = metricLock();
    for (NamedMeasurable m : stat.stats())
    {
        KafkaMetric metric = new KafkaMetric(lock, m.name(), m.stat(), config == null ? this.config : config, time);
    if (!metrics.containsKey(metric.metricName()))
    {
        registry.registerMetric(metric);
        metrics.put(metric.metricName(), metric);
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
    public bool add(MetricName metricName, MeasurableStat stat)
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
public synchronized bool add(MetricName metricName, MeasurableStat stat, MetricConfig config)
{
    if (hasExpired())
    {
        return false;
    }
    else if (metrics.containsKey(metricName))
    {
        return true;
    }
    else
    {
        KafkaMetric metric = new KafkaMetric(
            metricLock(),
            Utils.notNull(metricName),
            Utils.notNull(stat),
            config == null ? this.config : config,
            time
        );
        registry.registerMetric(metric);
        metrics.put(metric.metricName(), metric);
        stats.add(stat);
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

synchronized List<KafkaMetric> metrics() {
    return unmodifiableList(new LinkedList<>(this.metrics.values()));
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
private object metricLock()
{
    return metricLock;
}
}
}