using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Tasks;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Mocks
{
    public class MockChangelogReader : IChangelogRegister
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

        private void SetRestoredOffsets(Dictionary<TopicPartition, long> restoredOffsets)
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

        public void Remove(IEnumerable<TopicPartition> enumerable)
        {
            throw new System.NotImplementedException();
        }

        public void Register(TopicPartition partition, ProcessorStateManager stateManager)
        {
            throw new System.NotImplementedException();
        }
    }
}