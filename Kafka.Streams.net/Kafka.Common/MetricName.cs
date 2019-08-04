using System;
using System.Collections.Generic;

namespace Kafka.Common
{
    /**
     * The <code>MetricName</code> class encapsulates a metric's name, logical group and its related attributes. It should be constructed using metrics.MetricName(...).
     * <p>
     * This class captures the following parameters
     * <pre>
     *  <b>name</b> The name of the metric
     *  <b>group</b> logical group name of the metrics to which this metric belongs.
     *  <b>description</b> A human-readable description to include in the metric. This is optional.
     *  <b>tags</b>.Additional key/value attributes of the metric. This is optional.
     * </pre>
     * group, tags parameters can be used to create unique metric names while reporting in JMX or any custom reporting.
     * <p>
     * Ex: standard JMX MBean can be constructed like  <b>domainName:type=group,key1=val1,key2=val2</b>
     * <p>
     *
     * Usage looks something like this:
     * <pre>{@code
     * // set up metrics:
     *
     * Dictionary<string, string> metricTags = new LinkedHashMap<string, string>();
     * metricTags.Add("client-id", "producer-1");
     * metricTags.Add("topic", "topic");
     *
     * MetricConfig metricConfig = new MetricConfig().tags(metricTags);
     * Metrics metrics = new Metrics(metricConfig); // this is the global repository of metrics and sensors
     *
     * Sensor sensor = metrics.sensor("message-sizes");
     *
     * MetricName metricName = metrics.metricName("message-size-avg", "producer-metrics", "average message size");
     * sensor.Add(metricName, new Avg());
     *
     * metricName = metrics.metricName("message-size-max", "producer-metrics");
     * sensor.Add(metricName, new Max());
     *
     * metricName = metrics.metricName("message-size-min", "producer-metrics", "message minimum size", "client-id", "my-client", "topic", "my-topic");
     * sensor.Add(metricName, new Min());
     *
     * // as messages are sent we record the sizes
     * sensor.record(messageSize);
     * }</pre>
     */
    public class MetricName
    {

        private int hash = 0;

        /**
         * Please create MetricName by method {@link org.apache.kafka.common.metrics.Metrics#metricName(string, string, string, Map)}
         *
         * @param name        The name of the metric
         * @param group       logical group name of the metrics to which this metric belongs
         * @param description A human-readable description to include in the metric
         * @param tags       .Additional key/value attributes of the metric
         */
        public MetricName(string name, string group, string description, Dictionary<string, string> tags)
{
            this.Name = name ?? throw new ArgumentNullException(nameof(name));
            this.Group = group ?? throw new ArgumentNullException(nameof(group));
            this.Description = description ?? throw new ArgumentNullException(nameof(description));
            this.Tags = tags ?? throw new ArgumentNullException(nameof(tags));
        }

        public string Name { get; }

        public string Group { get; }

        public Dictionary<string, string> Tags { get; }

        public string Description { get; }

        public override int GetHashCode()
{
            if (hash != 0)
            {
                return hash;
            }

            int prime = 31;
            int result = 1;
            result = prime * result + Group.GetHashCode();
            result = prime * result + Name.GetHashCode();
            result = prime * result + Tags.GetHashCode();
            this.hash = result;

            return result;
        }

        public override bool Equals(object obj)
{
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (GetType() != obj.GetType())
                return false;
            MetricName other = (MetricName)obj;

            return Group.Equals(other.Group) && Name.Equals(other.Name) && Tags.Equals(other.Tags);
        }

        public override string ToString()
{
            return "MetricName [name=" + Name + ", group=" + Group + ", description="
                    + Description + ", tags=" +Tags + "]";
        }
    }
}