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
using Kafka.Streams.Processor.Internals;

namespace Kafka.Streams.State.Internals
{
    /**
     * A cache entry
     */
    public class LRUCacheEntry
    {
        private readonly ContextualRecord record;
        private readonly long sizeBytes;
        public bool isDirty { get; private set; }

        public LRUCacheEntry(byte[] value)
            : this(value, null, false, -1, -1, -1, "")
        {
        }

        LRUCacheEntry(byte[] value,
                      Headers headers,
                      bool isDirty,
                      long offset,
                      long timestamp,
                      int partition,
                      string topic)
        {
            ProcessorRecordContext context = new ProcessorRecordContext(timestamp, offset, partition, topic, headers);

            this.record = new ContextualRecord(
                value,
                context
            );

            this.isDirty = isDirty;
            this.sizeBytes = 1 + // isDirty
                record.residentMemorySizeEstimate();
        }

        public void markClean()
        {
            isDirty = false;
        }

        public long size()
        {
            return sizeBytes;
        }

        public byte[] value()
        {
            return record.value;
        }

        public ProcessorRecordContext context
        {
            get => record.recordContext;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            LRUCacheEntry that = (LRUCacheEntry)o;

            return sizeBytes == that.sizeBytes
                && isDirty == that.isDirty
                && record.Equals(that.record);
        }

        public override int GetHashCode()
        {
            return (record, sizeBytes, isDirty).GetHashCode();
        }
    }
}
