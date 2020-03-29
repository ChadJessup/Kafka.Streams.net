using Confluent.Kafka;
using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.Tests
{
    internal class MockStateRestoreListener : IStateRestoreListener
    {
        public void OnBatchRestored(TopicPartition topicPartition, string storeName, long batchEndOffset, long numRestored)
        {
        }

        public void OnRestoreEnd(TopicPartition topicPartition, string storeName, long totalRestored)
        {
        }

        public void OnRestoreStart(TopicPartition topicPartition, string storeName, long startingOffset, long endingOffset)
        {
        }
    }
}