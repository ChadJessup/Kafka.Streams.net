
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class TopicsInfo
    {
        public HashSet<string> sinkTopics { get; }
        public HashSet<string> sourceTopics { get; }
        public Dictionary<string, InternalTopicConfig> stateChangelogTopics;
        public Dictionary<string, InternalTopicConfig> repartitionSourceTopics;

        public TopicsInfo(
            HashSet<string> sinkTopics,
            HashSet<string> sourceTopics,
            Dictionary<string, InternalTopicConfig> repartitionSourceTopics,
            Dictionary<string, InternalTopicConfig> stateChangelogTopics)
        {
            this.sinkTopics = sinkTopics;
            this.sourceTopics = sourceTopics;
            this.stateChangelogTopics = stateChangelogTopics;
            this.repartitionSourceTopics = repartitionSourceTopics;
        }

        public override bool Equals(object o)
        {
            if (o is TopicsInfo)
            {
                return ((TopicsInfo)o).sourceTopics.Equals(this.sourceTopics) && ((TopicsInfo)o).stateChangelogTopics.Equals(this.stateChangelogTopics);
            }
            else
            {

                return false;
            }
        }

        public override int GetHashCode()
        {
            var n = ((long)this.sourceTopics.GetHashCode() << 32) | (long)this.stateChangelogTopics.GetHashCode();
            return (int)(n % 0xFFFFFFFFL);
        }

        public override string ToString()
        {
            return "TopicsInfo{" +
                "sinkTopics=" + this.sinkTopics +
                ", sourceTopics=" + this.sourceTopics +
                ", repartitionSourceTopics=" + this.repartitionSourceTopics +
                ", stateChangelogTopics=" + this.stateChangelogTopics +
                '}';
        }
    }
}
