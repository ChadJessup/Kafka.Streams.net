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

namespace Kafka.Streams.Processor.Internals
{
    public class StateRestoreCallbackAdapter
    {

        private StateRestoreCallbackAdapter() { }

        public static RecordBatchingStateRestoreCallback adapt(StateRestoreCallback restoreCallback)
        {
            restoreCallback = restoreCallback ?? throw new System.ArgumentNullException("stateRestoreCallback must not be null", nameof(restoreCallback));
            if (restoreCallback is RecordBatchingStateRestoreCallback)
            {
                return (RecordBatchingStateRestoreCallback)restoreCallback;
            }
            else if (restoreCallback is BatchingStateRestoreCallback)
            {
                return records-> {
                    List<KeyValue<byte[], byte[]>> keyValues = new List<>();
                    foreach (ConsumerRecord<byte[], byte[]> record in records)
                    {
                        keyValues.Add(new KeyValue<>(record.key(), record.value()));
                    }
                   ((BatchingStateRestoreCallback)restoreCallback).restoreAll(keyValues);
                };
            }
            else
            {

                //return records =>
                //{
                //    foreach (ConsumerRecord<byte[], byte[]> record in records)
                //    {
                //        restoreCallback.restore(record.key(), record.value());
                //    }
                //};
            }
        }
    }
}