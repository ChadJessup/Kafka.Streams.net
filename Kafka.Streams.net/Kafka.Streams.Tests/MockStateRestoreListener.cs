using Confluent.Kafka;
using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.Tests
{
    internal class MockStateRestoreListener : IStateRestoreListener
    {
        public void onBatchRestored(TopicPartition topicPartition, string storeName, long batchEndOffset, long numRestored)
        {
        }

        public void onRestoreEnd(TopicPartition topicPartition, string storeName, long totalRestored)
        {
        }

        public void onRestoreStart(TopicPartition topicPartition, string storeName, long startingOffset, long endingOffset)
        {
        }
    }
}