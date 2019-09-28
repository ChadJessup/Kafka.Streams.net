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

using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace Kafka.Streams.Processor.Internals
{
    public class StateRestoreCallbackAdapter
    {
        private StateRestoreCallbackAdapter() { }

        public static IRecordBatchingStateRestoreCallback adapt(IStateRestoreCallback restoreCallback)
        {
            restoreCallback = restoreCallback ?? throw new ArgumentNullException(nameof(restoreCallback));

            if (restoreCallback is IRecordBatchingStateRestoreCallback)
            {
                return (IRecordBatchingStateRestoreCallback)restoreCallback;
            }
            else if (restoreCallback is IBatchingStateRestoreCallback)
            {
                return new BatchingStateRestoreCallback(restoreCallback);
            }
            else
            {
                return new BasicRestoreCallback(restoreCallback);
            }
        }

        public class BasicRestoreCallback : IRecordBatchingStateRestoreCallback
        {
            private readonly IStateRestoreCallback restoreCallback;

            public BasicRestoreCallback(IStateRestoreCallback restoreCallback)
            {
                this.restoreCallback = restoreCallback;
            }

            public void restore(byte[] key, byte[] value)
            {
                throw new NotImplementedException();
            }

            public void restoreAll(List<KeyValue<byte[], byte[]>> records)
            {
                throw new NotImplementedException();
            }

            public void restoreBatch(List<ConsumeResult<byte[], byte[]>> records)
            {
                foreach (ConsumeResult<byte[], byte[]> record in records)
                {
                    this.restoreCallback.restore(record.Key, record.Value);
                }
            }
        }
    }
}