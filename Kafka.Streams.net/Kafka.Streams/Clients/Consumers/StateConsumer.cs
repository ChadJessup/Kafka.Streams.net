using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Internals;
using Microsoft.Extensions.Logging;
using NodaTime;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Clients.Consumers
{
    public class StateConsumer : BaseConsumer<byte[], byte[]>
    {
        private readonly ILogger<StateConsumer> logger;
        private readonly GlobalConsumer globalConsumer;
        private readonly IGlobalStateMaintainer stateMaintainer;
        private readonly IClock clock;
        private readonly TimeSpan pollTime;
        private readonly long flushInterval;

        private long lastFlush;

        public StateConsumer(
            ILogger<StateConsumer> logger,
            GlobalConsumer globalConsumer,
            IGlobalStateMaintainer stateMaintainer,
            IClock clock,
            TimeSpan pollTime,
            long flushInterval)
            : base(logger, null, null)
        {
            this.logger = logger;
            this.globalConsumer = globalConsumer;
            this.stateMaintainer = stateMaintainer;
            this.clock = clock;
            this.pollTime = pollTime;
            this.flushInterval = flushInterval;
        }

        /**
         * @throws InvalidOperationException If store gets registered after initialized is already finished
         * @throws StreamsException      if the store's change log does not contain the partition
         */
        public void Initialize()
        {
            var partitionOffsets = stateMaintainer.Initialize();
            globalConsumer.Assign(partitionOffsets.Keys);

            foreach (var entry in partitionOffsets)
            {
                globalConsumer.Seek(new TopicPartitionOffset(entry.Key, entry.Value ?? 0));
            }

            lastFlush = clock.GetCurrentInstant().ToUnixTimeMilliseconds();
        }

        public void PollAndUpdate()
        {
            try
            {
                ConsumerRecords<byte[], byte[]> received = globalConsumer.Poll(pollTime);

                foreach (ConsumeResult<byte[], byte[]> record in received)
                {
                    stateMaintainer.Update(record);
                }

                var now = clock.GetCurrentInstant().ToUnixTimeMilliseconds();
                if (now >= lastFlush + flushInterval)
                {
                    stateMaintainer.FlushState();
                    lastFlush = now;
                }
            }
            catch (TopicPartitionOffsetException recoverableException)
            {
                this.logger.LogError("Updating global state failed. You can restart KafkaStreams to recover from this error.", recoverableException);

                throw new StreamsException("Updating global state failed. " +
                    "You can restart KafkaStreams to recover from this error.", recoverableException);
            }
        }

        public override void Close()
        {
            try
            {
                globalConsumer.Close();
            }
            catch (RuntimeException e)
            {
                // just log an error if the consumerclose
                // so we can always attempt to close the state stores.
                this.logger.LogError("Failed to close global consumer due to the following error:", e);
            }

            stateMaintainer.Close();
        }
    }
}
