
using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Tasks;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Kafka.Streams.Tests.Internal
{
    public class MockChangelogReader : IChangelogReader
    {
        private readonly List<TopicPartition> registered = new List<TopicPartition>();
        private Dictionary<TopicPartition, long> restoredOffsets = new Dictionary<TopicPartition, long>();

        public Dictionary<TopicPartition, long> GetRestoredOffsets()
            => this.restoredOffsets;

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
            this.restoredOffsets = restoredOffsets;
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
