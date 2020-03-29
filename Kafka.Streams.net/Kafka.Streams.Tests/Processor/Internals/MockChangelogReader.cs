
using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Tasks;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Kafka.Streams.Tests.Internal
{
    public class MockChangelogReader : IChangelogReader
    {
        private List<TopicPartition> registered = new List<TopicPartition>();
        public Dictionary<TopicPartition, long> RestoredOffsets { get; private set; } = new Dictionary<TopicPartition, long>();

        public void register(StateRestorer restorer)
        {
            registered.Add(restorer.partition);
        }


        public List<TopicPartition> restore(IRestoringTasks active)
        {
            return registered;
        }

        void setRestoredOffsets(Dictionary<TopicPartition, long> restoredOffsets)
        {
            this.RestoredOffsets = restoredOffsets;
        }


        public void reset()
        {
            registered.Clear();
        }

        public bool wasRegistered(TopicPartition partition)
        {
            return registered.Contains(partition);
        }
    }
}
