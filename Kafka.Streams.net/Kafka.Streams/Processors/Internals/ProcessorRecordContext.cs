using Confluent.Kafka;
using Kafka.Common.Extensions;
using Kafka.Streams.KStream.Internals;
using System;
using System.Linq;
using System.Text;

namespace Kafka.Streams.Processors.Internals
{
    public class ProcessorRecordContext : IRecordContext
    {
        public DateTime Timestamp { get; }
        public long Offset { get; }
        public string Topic { get; }
        public int Partition { get; }
        public Headers Headers { get; }

        public ProcessorRecordContext(
            DateTime timestamp,
            long offset,
            int partition,
            string topic,
            Headers headers)
        {
            this.Timestamp = timestamp;
            this.Offset = offset;
            this.Topic = topic;
            this.Partition = partition;
            this.Headers = headers;
        }

        public long ResidentMemorySizeEstimate()
        {
            long size = 0;
            size += sizeof(long); // value.context.timestamp
            size += sizeof(long); // value.context.offset
            if (this.Topic != null)
            {
                size += this.Topic.ToCharArray().Length;
            }

            size += sizeof(int); // partition
            if (this.Headers != null)
            {
                foreach (Header header in this.Headers)
                {
                    size += header.Key.ToCharArray().Length;
                    var value = header.GetValueBytes();
                    if (value != null)
                    {
                        size += value.Length;
                    }
                }
            }
            return size;
        }

        public byte[] Serialize()
        {
            var topicBytes = Encoding.UTF8.GetBytes(this.Topic);
            byte[][] headerKeysBytes;
            byte[][] headerValuesBytes;

            var size = 0;
            size += sizeof(long); // value.context.timestamp
            size += sizeof(long); // value.context.offset
            size += sizeof(int); // size of topic
            size += topicBytes.Length;
            size += sizeof(int); // partition
            size += sizeof(int); // number of headers

            if (this.Headers == null)
            {
                headerKeysBytes = headerValuesBytes = null;
            }
            else
            {
                IHeader[] headers = this.Headers.ToArray();
                headerKeysBytes = new byte[headers.Length][];
                headerValuesBytes = new byte[headers.Length][];

                for (var i = 0; i < headers.Length; i++)
                {
                    size += 2 * sizeof(int); // sizes of key and value

                    var keyBytes = Encoding.UTF8.GetBytes(headers[i].Key);
                    size += keyBytes.Length;
                    var valueBytes = headers[i].GetValueBytes();
                    if (valueBytes != null)
                    {
                        size += valueBytes.Length;
                    }

                    headerKeysBytes[i] = keyBytes;
                    headerValuesBytes[i] = valueBytes;
                }
            }

            ByteBuffer buffer = new ByteBuffer().Allocate(size);
            buffer.PutLong(this.Timestamp.ToEpochMilliseconds());
            buffer.PutLong(this.Offset);

            // not handling the null condition because we believe topic will never be null in cases where we serialize
            buffer.PutInt(topicBytes.Length);
            buffer.Add(topicBytes);

            buffer.PutInt(this.Partition);
            if (this.Headers == null)
            {
                buffer.PutInt(-1);
            }
            else
            {

                buffer.PutInt(headerKeysBytes.Length);
                for (var i = 0; i < headerKeysBytes.Length; i++)
                {
                    buffer.PutInt(headerKeysBytes[i].Length);
                    buffer.Add(headerKeysBytes[i]);

                    if (headerValuesBytes[i] != null)
                    {
                        buffer.PutInt(headerValuesBytes[i].Length);
                        buffer.Add(headerValuesBytes[i]);
                    }
                    else
                    {

                        buffer.PutInt(-1);
                    }
                }
            }

            return buffer.Array();
        }

        public static ProcessorRecordContext Deserialize(ByteBuffer buffer)
        {
            var timestamp = Confluent.Kafka.Timestamp.UnixTimestampMsToDateTime(buffer.GetLong());

            var offset = buffer.GetLong();
            var topicSize = buffer.GetInt();

            string topic;

            {
                // not handling the null topic condition, because we believe the topic will never be null when we serialize
                var topicBytes = new byte[topicSize];
                buffer.Get(topicBytes);
                topic = new string(topicBytes.Cast<char>().ToArray());
            }

            var partition = buffer.GetInt();
            var headerCount = buffer.GetInt();
            Headers headers;
            if (headerCount == -1)
            {
                headers = null;
            }
            else
            {
                var headerArr = new Header[headerCount];

                for (var i = 0; i < headerCount; i++)
                {
                    var keySize = buffer.GetInt();
                    var keyBytes = new byte[keySize];
                    buffer.Get(keyBytes);

                    var valueSize = buffer.GetInt();
                    byte[] valueBytes;
                    if (valueSize == -1)
                    {
                        valueBytes = null;
                    }
                    else
                    {

                        valueBytes = new byte[valueSize];
                        buffer.Get(valueBytes);
                    }

                    headerArr[i] = new Header(new string(keyBytes.Cast<char>().ToArray()), valueBytes);
                }

                headers = new Headers();
                foreach (var header in headerArr)
                {
                    headers.Add(header);
                }
            }

            return new ProcessorRecordContext(timestamp, offset, partition, topic, headers);
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            var that = (ProcessorRecordContext)o;

            return this.Timestamp == that.Timestamp &&
                this.Offset == that.Offset &&
                this.Partition == that.Partition &&
                this.Topic.Equals(that.Topic) &&
                this.Headers.Equals(that.Headers);
        }

        public override string ToString()
        {
            return "ProcessorRecordContext{" +
                "topic='" + this.Topic + '\'' +
                ", partition=" + this.Partition +
                ", offset=" + this.Offset +
                ", timestamp=" + this.Timestamp +
                ", headers=" + this.Headers +
                '}';
        }

        public override int GetHashCode()
            => base.GetHashCode();
    }
}
