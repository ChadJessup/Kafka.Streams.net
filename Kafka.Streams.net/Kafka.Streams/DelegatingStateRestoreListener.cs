/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
using Kafka.Streams.Errors;
using Kafka.Streams.Processor.Interfaces;
using System;

namespace Kafka.Streams
{
    public class DelegatingStateRestoreListener : IStateRestoreListener
    {
        private void throwOnFatalException(
            Exception fatalUserException,
            TopicPartition topicPartition,
            string storeName)
        {
            throw new StreamsException(
                    string.Format("Fatal user code error in store restore listener for store %s, partition %s.",
                            storeName,
                            topicPartition),
                    fatalUserException);
        }

        public void onRestoreStart(
            TopicPartition topicPartition,
            string storeName,
            long startingOffset,
            long endingOffset)
        {
            if (globalStateRestoreListener != null)
            {
                try
                {
                    globalStateRestoreListener.onRestoreStart(topicPartition, storeName, startingOffset, endingOffset);
                }
                catch (Exception fatalUserException)
                {
                    throwOnFatalException(fatalUserException, topicPartition, storeName);
                }
            }
        }

        public void onBatchRestored(
            TopicPartition topicPartition,
            string storeName,
            long batchEndOffset,
            long numRestored)
        {
            if (globalStateRestoreListener != null)
            {
                try
                {
                    globalStateRestoreListener.onBatchRestored(topicPartition, storeName, batchEndOffset, numRestored);
                }
                catch (Exception fatalUserException)
                {
                    throwOnFatalException(fatalUserException, topicPartition, storeName);
                }
            }
        }

        public void onRestoreEnd(TopicPartition topicPartition, string storeName, long totalRestored)
        {
            if (globalStateRestoreListener != null)
            {
                try
                {
                    globalStateRestoreListener.onRestoreEnd(topicPartition, storeName, totalRestored);
                }
                catch (Exception fatalUserException)
                {
                    throwOnFatalException(fatalUserException, topicPartition, storeName);
                }
            }
        }
    }
}
