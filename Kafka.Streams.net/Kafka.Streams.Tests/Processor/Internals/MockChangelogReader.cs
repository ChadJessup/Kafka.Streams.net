
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
            this.registered.Add(restorer.partition);
        }


        public List<TopicPartition> Restore(IRestoringTasks active)
        {
            return this.registered;
        }

        void SetRestoredOffsets(Dictionary<TopicPartition, long> restoredOffsets)
        {
            this.restoredOffsets = restoredOffsets;
        }
        
        public void Reset()
        {
            this.registered.Clear();
        }

        public bool WasRegistered(TopicPartition partition)
        {
            return this.registered.Contains(partition);
        }
    }
}
