using Kafka.Common;
using Kafka.Streams.Threads.KafkaStream;
using NodaTime;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class MockStreamThreadConsumer<K, V> : MockConsumer<K, V>
    {

        private KafkaStreamThread streamThread;

        private MockStreamThreadConsumer()//OffsetResetStrategy offsetResetStrategy)
            : base(null)//offsetResetStrategy)
        {
        }

        public ConsumerRecords<K, V> poll(Duration timeout)
        {
            Assert.NotNull(streamThread);
            //if (shutdownOnPoll)
            {
                //  streamThread.Shutdown();
            }

            //streamThread.RebalanceListener.OnPartitionsAssigned(null, assignedPartitions);
            return null;// base.Poll(timeout);
        }

        private void setStreamThread(KafkaStreamThread streamThread)
        {
            this.streamThread = streamThread;
        }
    }
}
