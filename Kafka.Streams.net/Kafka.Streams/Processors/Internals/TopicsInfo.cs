
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
            var n = ((long)sourceTopics.GetHashCode() << 32) | (long)stateChangelogTopics.GetHashCode();
            return (int)(n % 0xFFFFFFFFL);
        }

        public override string ToString()
        {
            return "TopicsInfo{" +
                "sinkTopics=" + sinkTopics +
                ", sourceTopics=" + sourceTopics +
                ", repartitionSourceTopics=" + repartitionSourceTopics +
                ", stateChangelogTopics=" + stateChangelogTopics +
                '}';
        }
    }
}
