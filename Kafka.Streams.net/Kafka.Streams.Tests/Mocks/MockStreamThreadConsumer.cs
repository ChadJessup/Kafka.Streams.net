﻿using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Threads.Stream;

using System;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class MockStreamThreadConsumer<K, V> : MockConsumer<K, V>
    {

        private StreamThread streamThread;

        private MockStreamThreadConsumer()//OffsetResetStrategy offsetResetStrategy)
            : base(null)//offsetResetStrategy)
        {
        }

        public ConsumeResult<K, V> Poll(TimeSpan timeout)
        {
            Assert.NotNull(this.streamThread);
            //if (shutdownOnPoll)
            {
                //  streamThread.Shutdown();
            }

            //streamThread.RebalanceListener.OnPartitionsAssigned(null,assignedPartitions);
            return null;// base.Poll(timeout);
        }

        private void SetStreamThread(StreamThread streamThread)
        {
            this.streamThread = streamThread;
        }
    }
}
