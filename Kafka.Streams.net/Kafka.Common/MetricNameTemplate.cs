using System;
using System.Collections.Generic;

namespace Kafka.Common
{
    /**
     * A template for a MetricName. It contains a name, group, and description, as
     * well as all the tags that will be used to create the mBean name. Tag values
     * are omitted from the template, but are filled in at runtime with their
     * specified values. The order of the tags is maintained, if an ordered set
     * is provided, so that the mBean names can be compared and sorted lexicographically.
     */
    public class MetricNameTemplate
    {
        public string name { get; set; }
        public string group { get; set; }
        public string description { get; set; }
        public HashSet<string> tags { get; set; }

        /**
         * Create a new template. Note that the order of the tags will be preserved if the supplied
         * {@code tagsNames} set has an order.
         *
         * @param name the name of the metric; may not be null
         * @param group the name of the group; may not be null
         * @param description the description of the metric; may not be null
         * @param tagsNames the set of metric tag names, which can/should be a set that maintains order; may not be null
         */
        public MetricNameTemplate(string name, string group, string description, HashSet<string> tagsNames)
        {
            this.name = name ?? throw new ArgumentNullException(nameof(name));
            this.group = group ?? throw new ArgumentNullException(nameof(group));
            this.description = description ?? throw new ArgumentNullException(nameof(description));
            this.tags = new HashSet<string>(tagsNames ?? throw new ArgumentNullException(nameof(tagsNames)));
        }

        /**
         * Create a new template. Note that the order of the tags will be preserved.
         *
         * @param name the name of the metric; may not be null
         * @param group the name of the group; may not be null
         * @param description the description of the metric; may not be null
         * @param tagsNames the names of the metric tags in the preferred order; none of the tag names should be null
         */
        public MetricNameTemplate(string name, string group, string description, string[] tagsNames)
           : this(name, group, description, getTags(tagsNames))
        {
        }

        private static HashSet<string> getTags(string[] keys)
        {
            HashSet<string> tags = new HashSet<string>();

            //            Collections.addAll(tags, keys);

            return tags;
        }

        public override int GetHashCode()
        {
            return (name, group, tags).GetHashCode();
        }

        public override bool Equals(object o)
        {
            if (this == o)
                return true;
            if (o == null || GetType() != o.GetType())
                return false;

            MetricNameTemplate other = (MetricNameTemplate)o;

            return name.Equals(other.name)
                && group.Equals(other.group)
                && tags.Equals(other.tags);
        }

        public override string ToString()
        {
            return string.Format("name=%s, group=%s, tags=%s", name, group, tags);
        }
    }
}