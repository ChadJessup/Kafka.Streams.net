﻿using Confluent.Kafka;
using Kafka.Common;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Internals;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Clients.Consumers
{
    public class StateConsumer : BaseConsumer<byte[], byte[]>
    {
        private readonly ILogger<StateConsumer> logger;
        private readonly GlobalConsumer globalConsumer;
        private readonly IGlobalStateMaintainer stateMaintainer;
        private readonly ITime time;
        private readonly TimeSpan pollTime;
        private readonly long flushInterval;

        private long lastFlush;

        public StateConsumer(
            ILogger<StateConsumer> logger,
            GlobalConsumer globalConsumer,
            IGlobalStateMaintainer stateMaintainer,
            ITime time,
            TimeSpan pollTime,
            long flushInterval)
            : base(logger, null, null)
        {
            this.logger = logger;
            this.globalConsumer = globalConsumer;
            this.stateMaintainer = stateMaintainer;
            this.time = time;
            this.pollTime = pollTime;
            this.flushInterval = flushInterval;
        }

        /**
         * @throws InvalidOperationException If store gets registered after initialized is already finished
         * @throws StreamsException      if the store's change log does not contain the partition
         */
        public void initialize()
        {
            Dictionary<TopicPartition, long> partitionOffsets = stateMaintainer.initialize();
            globalConsumer.Assign(partitionOffsets.Keys);

            foreach (KeyValuePair<TopicPartition, long> entry in partitionOffsets)
            {
                globalConsumer.Seek(new TopicPartitionOffset(entry.Key, entry.Value));
            }

            lastFlush = time.milliseconds();
        }

        public void pollAndUpdate()
        {
            try
            {
                ConsumerRecords<byte[], byte[]> received = globalConsumer.Poll(pollTime);

                foreach (ConsumeResult<byte[], byte[]> record in received)
                {
                    stateMaintainer.update(record);
                }

                long now = time.milliseconds();
                if (now >= lastFlush + flushInterval)
                {
                    stateMaintainer.flushState();
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

        public void close()
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

            stateMaintainer.close();
        }
    }
}
