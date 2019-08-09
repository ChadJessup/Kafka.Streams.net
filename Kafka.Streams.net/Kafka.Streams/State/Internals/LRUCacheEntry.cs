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
namespace Kafka.Streams.State.Internals
{


using Kafka.Common.header.Headers;
using Kafka.Streams.Processor.Internals.ProcessorRecordContext;



/**
 * A cache entry
 */
public class LRUCacheEntry
{
    private ContextualRecord record;
    private long sizeBytes;
    private bool isDirty;


    LRUCacheEntry(byte[] value)
{
        this(value, null, false, -1, -1, -1, "");
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

    void markClean()
{
        isDirty = false;
    }

    bool isDirty()
{
        return isDirty;
    }

    long size()
{
        return sizeBytes;
    }

    byte[] value()
{
        return record.value();
    }

    public ProcessorRecordContext context
{
        return record.recordContext();
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
        LRUCacheEntry that = (LRUCacheEntry) o;
        return sizeBytes == that.sizeBytes &&
            isDirty() == that.isDirty() &&
            Objects.Equals(record, that.record);
    }

    public override int GetHashCode()
{
        return Objects.hash(record, sizeBytes, isDirty());
    }
}
