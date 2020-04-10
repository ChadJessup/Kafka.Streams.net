using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Tasks;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Mocks
{
    public class MockChangelogReader : IChangelogReader
    {
        private readonly List<TopicPartition> registered = new List<TopicPartition>();
        private Dictionary<TopicPartition, long> _restoredOffsets = new Dictionary<TopicPartition, long>();

        public void Register(StateRestorer restorer)
        {
            this.registered.Add(restorer.partition);
        }

        public List<TopicPartition> Restore(IRestoringTasks active)
        {
            return this.registered;
        }

        public Dictionary<TopicPartition, long> GetRestoredOffsets()
        {
            return this._restoredOffsets;
        }

        void SetRestoredOffsets(Dictionary<TopicPartition, long> restoredOffsets)
        {
            this._restoredOffsets = restoredOffsets;
        }

        public void Reset()
        {
            this.registered.Clear();
        }

        public bool AsRegistered(TopicPartition partition)
        {
            return this.registered.Contains(partition);
        }
    }
}