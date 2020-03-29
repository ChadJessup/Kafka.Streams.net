
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

        public void Register(StateRestorer restorer)
        {
            registered.Add(restorer.partition);
        }


        public List<TopicPartition> Restore(IRestoringTasks active)
        {
            return registered;
        }

        void SetRestoredOffsets(Dictionary<TopicPartition, long> restoredOffsets)
        {
            this.RestoredOffsets = restoredOffsets;
        }


        public void Reset()
        {
            registered.Clear();
        }

        public bool WasRegistered(TopicPartition partition)
        {
            return registered.Contains(partition);
        }
    }
}
