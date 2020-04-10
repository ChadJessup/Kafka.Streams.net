using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Internals;
using Microsoft.Extensions.Logging;
using System;

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
            var partitionOffsets = this.stateMaintainer.Initialize();
            this.globalConsumer.Assign(partitionOffsets.Keys);

            foreach (var entry in partitionOffsets)
            {
                this.globalConsumer.Seek(new TopicPartitionOffset(entry.Key, entry.Value ?? 0));
            }

            this.lastFlush = this.clock.NowAsEpochMilliseconds;
        }

        public void PollAndUpdate()
        {
            try
            {
                ConsumerRecords<byte[], byte[]> received = this.globalConsumer.Poll(this.pollTime);

                foreach (ConsumeResult<byte[], byte[]> record in received)
                {
                    this.stateMaintainer.Update(record);
                }

                var now = this.clock.NowAsEpochMilliseconds;
                if (now >= this.lastFlush + this.flushInterval)
                {
                    this.stateMaintainer.FlushState();
                    this.lastFlush = now;
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
                this.globalConsumer.Close();
            }
            catch (RuntimeException e)
            {
                // just log an error if the consumerclose
                // so we can always attempt to Close the state stores.
                this.logger.LogError("Failed to Close global consumer due to the following error:", e);
            }

            this.stateMaintainer.Close();
        }
    }
}
