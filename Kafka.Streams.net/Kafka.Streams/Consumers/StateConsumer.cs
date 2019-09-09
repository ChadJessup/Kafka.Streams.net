/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Confluent.Kafka;
using Kafka.Common;
using Kafka.Common.Extensions;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Errors;
using Kafka.Streams.Processor.Internals;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Consumer
{
    public class StateConsumer
    {
        private readonly ILogger<StateConsumer> logger;
        private IConsumer<byte[], byte[]> globalConsumer;
        private IGlobalStateMaintainer stateMaintainer;
        private ITime time;
        private TimeSpan pollTime;
        private long flushInterval;

        private long lastFlush;

        public StateConsumer(
            ILogger<StateConsumer> logger,
            IConsumer<byte[], byte[]> globalConsumer,
            IGlobalStateMaintainer stateMaintainer,
            ITime time,
            TimeSpan pollTime,
            long flushInterval)
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
                ConsumerRecords<byte[], byte[]> received = globalConsumer.poll(pollTime);

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
