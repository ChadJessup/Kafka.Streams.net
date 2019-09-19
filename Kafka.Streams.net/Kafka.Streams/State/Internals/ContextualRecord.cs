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
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processor.Internals;
using System;

namespace Kafka.Streams.State.Internals
{
    public class ContextualRecord
    {
        public byte[] value { get; }
        public ProcessorRecordContext recordContext { get; }

        public ContextualRecord(byte[] value, ProcessorRecordContext recordContext)
        {
            this.recordContext = recordContext ?? throw new ArgumentNullException(nameof(recordContext));

            this.value = value;
        }

        public long residentMemorySizeEstimate()
        {
            return (value == null ? 0 : value.Length) + recordContext.residentMemorySizeEstimate();
        }

        ByteBuffer serialize(int intValue)
        {
            byte[] serializedContext = recordContext.Serialize();

            int sizeOfContext = serializedContext.Length;
            int sizeOfValueLength = sizeof(int);
            int sizeOfValue = value == null ? 0 : value.Length;
            ByteBuffer buffer = new ByteBuffer();//.allocate(sizeOfContext + sizeOfValueLength + sizeOfValue + end.Adding);

            buffer.Add(serializedContext);

            if (value == null)
            {
                buffer.putInt(-1);
            }
            else
            {
                buffer.putInt(value.Length);
                buffer.Add(value);
            }

            return buffer;
        }

        static ContextualRecord deserialize(ByteBuffer buffer)
        {
            ProcessorRecordContext context = ProcessorRecordContext.Deserialize(buffer);

            int valueLength = buffer.getInt();
            if (valueLength == -1)
            {
                return new ContextualRecord(null, context);
            }
            else
            {
                byte[] value = new byte[valueLength];
                buffer.get(value);

                return new ContextualRecord(value, context);
            }
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

            ContextualRecord that = (ContextualRecord)o;

            return value.Equals(that.value)
                && recordContext.Equals(that.recordContext);
        }

        public override int GetHashCode()
        {
            return (value, recordContext).GetHashCode();
        }

        public override string ToString()
        {
            return "ContextualRecord{" +
                "recordContext=" + recordContext +
                ", value=" + value +
                '}';
        }
    }
}