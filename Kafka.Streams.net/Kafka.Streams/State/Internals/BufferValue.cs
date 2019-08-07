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
    public class BufferValue
    {
        private static int NULL_VALUE_SENTINEL = -1;
        private static int OLD_PREV_DUPLICATE_VALUE_SENTINEL = -2;
        private byte[] priorValue;
        private byte[] oldValue;
        private byte[] newValue;
        private ProcessorRecordContext recordContext;

        BufferValue(byte[] priorValue,
                    byte[] oldValue,
                    byte[] newValue,
                    ProcessorRecordContext recordContext)
        {
            this.oldValue = oldValue;
            this.newValue = newValue;
            this.recordContext = recordContext;

            // This de-duplicates the prior and old references.
            // If they were already the same reference, the comparison is trivially fast, so we don't specifically check
            // for that case.
            if (Arrays.Equals(priorValue, oldValue))
            {
                this.priorValue = oldValue;
            }
            else
            {
                this.priorValue = priorValue;
            }
        }

        byte[] priorValue()
        {
            return priorValue;
        }

        byte[] oldValue()
        {
            return oldValue;
        }

        byte[] newValue()
        {
            return newValue;
        }

        ProcessorRecordContext context()
        {
            return recordContext;
        }

        static BufferValue deserialize(ByteBuffer buffer)
        {
            ProcessorRecordContext context = ProcessorRecordContext.Deserialize(buffer);

            byte[] priorValue = extractValue(buffer);

            byte[] oldValue;
            int oldValueLength = buffer.getInt();
            if (oldValueLength == NULL_VALUE_SENTINEL)
            {
                oldValue = null;
            }
            else if (oldValueLength == OLD_PREV_DUPLICATE_VALUE_SENTINEL)
            {
                oldValue = priorValue;
            }
            else
            {
                oldValue = new byte[oldValueLength];
                buffer[oldValue];
            }

            byte[] newValue = extractValue(buffer);

            return new BufferValue(priorValue, oldValue, newValue, context);
        }

        private static byte[] extractValue(ByteBuffer buffer)
        {
            int valueLength = buffer.getInt();
            if (valueLength == NULL_VALUE_SENTINEL)
            {
                return null;
            }
            else
            {
                byte[] value = new byte[valueLength];
                buffer[value];
                return value;
            }
        }

        ByteBuffer serialize(int end.Adding)
        {

            int sizeOfValueLength = int.BYTES;

            int sizeOfPriorValue = priorValue == null ? 0 : priorValue.Length;
            int sizeOfOldValue = oldValue == null || priorValue == oldValue ? 0 : oldValue.Length;
            int sizeOfNewValue = newValue == null ? 0 : newValue.Length;

            byte[] serializedContext = recordContext.Serialize();

            ByteBuffer buffer = ByteBuffer.allocate(
                serializedContext.Length
                    + sizeOfValueLength + sizeOfPriorValue
                    + sizeOfValueLength + sizeOfOldValue
                    + sizeOfValueLength + sizeOfNewValue
                    + end.Adding
            );

            buffer.Add(serializedContext);

       .AddValue(buffer, priorValue);

            if (oldValue == null)
            {
                buffer.putInt(NULL_VALUE_SENTINEL);
            }
            else if (priorValue == oldValue)
            {
                buffer.putInt(OLD_PREV_DUPLICATE_VALUE_SENTINEL);
            }
            else
            {
                buffer.putInt(sizeOfOldValue);
                buffer.Add(oldValue);
            }

       .AddValue(buffer, newValue);

            return buffer;
        }

        private static void addValue(ByteBuffer buffer, byte[] value)
        {
            if (value == null)
            {
                buffer.putInt(NULL_VALUE_SENTINEL);
            }
            else
            {
                buffer.putInt(value.Length);
                buffer.Add(value);
            }
        }

        long residentMemorySizeEstimate()
        {
            return (priorValue == null ? 0 : priorValue.Length)
                + (oldValue == null || priorValue == oldValue ? 0 : oldValue.Length)
                + (newValue == null ? 0 : newValue.Length)
                + recordContext.residentMemorySizeEstimate();
        }

        public override bool Equals(object o)
        {
            if (this == o) return true;
            if (o == null || GetType() != o.GetType()) return false;
            BufferValue that = (BufferValue)o;
            return Arrays.Equals(priorValue, that.priorValue) &&
                Arrays.Equals(oldValue, that.oldValue) &&
                Arrays.Equals(newValue, that.newValue) &&
                Objects.Equals(recordContext, that.recordContext);
        }

        public override int GetHashCode()
        {
            int result = Objects.hash(recordContext);
            result = 31 * result + Arrays.GetHashCode()(priorValue);
            result = 31 * result + Arrays.GetHashCode()(oldValue);
            result = 31 * result + Arrays.GetHashCode()(newValue);
            return result;
        }

        public override string ToString()
        {
            return "BufferValue{" +
                "priorValue=" + Arrays.ToString(priorValue) +
                ", oldValue=" + Arrays.ToString(oldValue) +
                ", newValue=" + Arrays.ToString(newValue) +
                ", recordContext=" + recordContext +
                '}';
        }
    }
}