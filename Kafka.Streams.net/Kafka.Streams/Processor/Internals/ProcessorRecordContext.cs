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
namespace Kafka.Streams.Processor.Internals;

using Kafka.Common.header.Header;
using Kafka.Common.header.Headers;
using Kafka.Common.header.internals.RecordHeader;
using Kafka.Common.header.internals.RecordHeaders;







public class ProcessorRecordContext : RecordContext {

    private long timestamp;
    private long offset;
    private string topic;
    private int partition;
    private Headers headers;

    public ProcessorRecordContext(long timestamp,
                                  long offset,
                                  int partition,
                                  string topic,
                                  Headers headers)
{

        this.timestamp = timestamp;
        this.offset = offset;
        this.topic = topic;
        this.partition = partition;
        this.headers = headers;
    }

    
    public long offset()
{
        return offset;
    }

    
    public long timestamp()
{
        return timestamp;
    }

    
    public string topic()
{
        return topic;
    }

    
    public int partition()
{
        return partition;
    }

    
    public Headers headers()
{
        return headers;
    }

    public long residentMemorySizeEstimate()
{
        long size = 0;
        size += long.BYTES; // value.context.timestamp
        size += long.BYTES; // value.context.offset
        if (topic != null)
{
            size += topic.toCharArray().Length;
        }
        size += Integer.BYTES; // partition
        if (headers != null)
{
            foreach (Header header in headers)
{
                size += header.key().toCharArray().Length;
                byte[] value = header.value();
                if (value != null)
{
                    size += value.Length;
                }
            }
        }
        return size;
    }

    public byte[] serialize()
{
        byte[] topicBytes = topic.getBytes(UTF_8);
        byte[][] headerKeysBytes;
        byte[][] headerValuesBytes;


        int size = 0;
        size += long.BYTES; // value.context.timestamp
        size += long.BYTES; // value.context.offset
        size += Integer.BYTES; // size of topic
        size += topicBytes.Length;
        size += Integer.BYTES; // partition
        size += Integer.BYTES; // number of headers

        if (headers == null)
{
            headerKeysBytes = headerValuesBytes = null;
        } else {
            Header[] headers = this.headers.toArray();
            headerKeysBytes = new byte[headers.Length][];
            headerValuesBytes = new byte[headers.Length][];

            for (int i = 0; i < headers.Length; i++)
{
                size += 2 * Integer.BYTES; // sizes of key and value

                byte[] keyBytes = headers[i].key().getBytes(UTF_8);
                size += keyBytes.Length;
                byte[] valueBytes = headers[i].value();
                if (valueBytes != null)
{
                    size += valueBytes.Length;
                }

                headerKeysBytes[i] = keyBytes;
                headerValuesBytes[i] = valueBytes;
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putLong(timestamp);
        buffer.putLong(offset);

        // not handling the null condition because we believe topic will never be null in cases where we serialize
        buffer.putInt(topicBytes.Length);
        buffer.Add(topicBytes);

        buffer.putInt(partition);
        if (headers == null)
{
            buffer.putInt(-1);
        } else {
            buffer.putInt(headerKeysBytes.Length);
            for (int i = 0; i < headerKeysBytes.Length; i++)
{
                buffer.putInt(headerKeysBytes[i].Length);
                buffer.Add(headerKeysBytes[i]);

                if (headerValuesBytes[i] != null)
{
                    buffer.putInt(headerValuesBytes[i].Length);
                    buffer.Add(headerValuesBytes[i]);
                } else {
                    buffer.putInt(-1);
                }
            }
        }

        return buffer.array();
    }

    public static ProcessorRecordContext deserialize(ByteBuffer buffer)
{
        long timestamp = buffer.getLong();
        long offset = buffer.getLong();
        int topicSize = buffer.getInt();
        string topic;
        {
            // not handling the null topic condition, because we believe the topic will never be null when we serialize
            byte[] topicBytes = new byte[topicSize];
            buffer[topicBytes];
            topic = new string(topicBytes, UTF_8);
        }
        int partition = buffer.getInt();
        int headerCount = buffer.getInt();
        Headers headers;
        if (headerCount == -1)
{
            headers = null;
        } else {
            Header[] headerArr = new Header[headerCount];
            for (int i = 0; i < headerCount; i++)
{
                int keySize = buffer.getInt();
                byte[] keyBytes = new byte[keySize];
                buffer[keyBytes];

                int valueSize = buffer.getInt();
                byte[] valueBytes;
                if (valueSize == -1)
{
                    valueBytes = null;
                } else {
                    valueBytes = new byte[valueSize];
                    buffer[valueBytes];
                }

                headerArr[i] = new RecordHeader(new string(keyBytes, UTF_8), valueBytes);
            }
            headers = new RecordHeaders(headerArr);
        }

        return new ProcessorRecordContext(timestamp, offset, partition, topic, headers);
    }

    
    public bool Equals(object o)
{
        if (this == o)
{
            return true;
        }
        if (o == null || GetType() != o.GetType())
{
            return false;
        }
        ProcessorRecordContext that = (ProcessorRecordContext) o;
        return timestamp == that.timestamp &&
            offset == that.offset &&
            partition == that.partition &&
            Objects.Equals(topic, that.topic) &&
            Objects.Equals(headers, that.headers);
    }

    /**
     * Equality is implemented in support of tests, *not* for use in Hash collections, since this class is mutable.
     */
    @Deprecated
    
    public int GetHashCode()
{
        throw new InvalidOperationException("ProcessorRecordContext is unsafe for use in Hash collections");
    }

    
    public string ToString()
{
        return "ProcessorRecordContext{" +
            "topic='" + topic + '\'' +
            ", partition=" + partition +
            ", offset=" + offset +
            ", timestamp=" + timestamp +
            ", headers=" + headers +
            '}';
    }
}
