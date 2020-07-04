using System;
using Confluent.Kafka;
using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.Threads.KafkaStreams
{
    public class WrappedStateRestoreListener<States> : IStateRestoreListener
        where States : Enum
    {
        private readonly Action<IThread<States>, States, States> globalStateRestoreListener;

        public WrappedStateRestoreListener(Action<IThread<States>, States, States> globalStateRestoreListener)
        {
            this.globalStateRestoreListener = globalStateRestoreListener;
        }

        public void OnBatchRestored(TopicPartition topicPartition, string storeName, long batchEndOffset, long numRestored)
        {
            throw new NotImplementedException();
        }

        public void OnRestoreEnd(TopicPartition topicPartition, string storeName, long totalRestored)
        {
            throw new NotImplementedException();
        }

        public void OnRestoreStart(TopicPartition topicPartition, string storeName, long startingOffset, long endingOffset)
        {
            throw new NotImplementedException();
        }
    }
}
