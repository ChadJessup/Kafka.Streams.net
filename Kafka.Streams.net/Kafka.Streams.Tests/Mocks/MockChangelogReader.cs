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
            registered.Add(restorer.partition);
        }

        public List<TopicPartition> Restore(IRestoringTasks active)
        {
            return registered;
        }

        public Dictionary<TopicPartition, long> GetRestoredOffsets()
        {
            return _restoredOffsets;
        }

        void SetRestoredOffsets(Dictionary<TopicPartition, long> restoredOffsets)
        {
            this._restoredOffsets = restoredOffsets;
        }

        public void Reset()
        {
            registered.Clear();
        }

        public bool AsRegistered(TopicPartition partition)
        {
            return registered.Contains(partition);
        }
    }
}