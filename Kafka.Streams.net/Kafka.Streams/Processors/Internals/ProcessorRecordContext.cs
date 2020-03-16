using Confluent.Kafka;
using Kafka.Streams.KStream.Internals;
using System.Linq;
using System.Text;

namespace Kafka.Streams.Processors.Internals
{
    public class ProcessorRecordContext : IRecordContext
    {
        public long timestamp { get; }
        public long offset { get; }
        public string Topic { get; }
        public int partition { get; }
        public Headers headers { get; }

        public ProcessorRecordContext(
            long timestamp,
            long offset,
            int partition,
            string topic,
            Headers headers)
        {
            this.timestamp = timestamp;
            this.offset = offset;
            this.Topic = topic;
            this.partition = partition;
            this.headers = headers;
        }

        public long residentMemorySizeEstimate()
        {
            long size = 0;
            size += sizeof(long); // value.context.timestamp
            size += sizeof(long); // value.context.offset
            if (Topic != null)
            {
                size += Topic.ToCharArray().Length;
            }
            size += sizeof(int); // partition
            if (headers != null)
            {
                foreach (Header header in headers)
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
            var topicBytes = Encoding.UTF8.GetBytes(Topic);
            byte[][] headerKeysBytes;
            byte[][] headerValuesBytes;


            var size = 0;
            size += sizeof(long); // value.context.timestamp
            size += sizeof(long); // value.context.offset
            size += sizeof(int); // size of topic
            size += topicBytes.Length;
            size += sizeof(int); // partition
            size += sizeof(int); // number of headers

            if (headers == null)
            {
                headerKeysBytes = headerValuesBytes = null;
            }
            else
            {
                IHeader[] headers = this.headers.ToArray();
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

            ByteBuffer buffer = new ByteBuffer().allocate(size);
            buffer.putLong(timestamp);
            buffer.putLong(offset);

            // not handling the null condition because we believe topic will never be null in cases where we serialize
            buffer.putInt(topicBytes.Length);
            buffer.Add(topicBytes);

            buffer.putInt(partition);
            if (headers == null)
            {
                buffer.putInt(-1);
            }
            else
            {

                buffer.putInt(headerKeysBytes.Length);
                for (var i = 0; i < headerKeysBytes.Length; i++)
                {
                    buffer.putInt(headerKeysBytes[i].Length);
                    buffer.Add(headerKeysBytes[i]);

                    if (headerValuesBytes[i] != null)
                    {
                        buffer.putInt(headerValuesBytes[i].Length);
                        buffer.Add(headerValuesBytes[i]);
                    }
                    else
                    {

                        buffer.putInt(-1);
                    }
                }
            }

            return buffer.array();
        }

        public static ProcessorRecordContext Deserialize(ByteBuffer buffer)
        {
            var timestamp = buffer.getLong();
            var offset = buffer.getLong();
            var topicSize = buffer.getInt();
            string topic;
            {
                // not handling the null topic condition, because we believe the topic will never be null when we serialize
                var topicBytes = new byte[topicSize];
                buffer.get(topicBytes);
                topic = new string(topicBytes.Cast<char>().ToArray());
            }
            var partition = buffer.getInt();
            var headerCount = buffer.getInt();
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
                    var keySize = buffer.getInt();
                    var keyBytes = new byte[keySize];
                    buffer.get(keyBytes);

                    var valueSize = buffer.getInt();
                    byte[] valueBytes;
                    if (valueSize == -1)
                    {
                        valueBytes = null;
                    }
                    else
                    {

                        valueBytes = new byte[valueSize];
                        buffer.get(valueBytes);
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

            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            var that = (ProcessorRecordContext)o;

            return timestamp == that.timestamp &&
                offset == that.offset &&
                partition == that.partition &&
                Topic.Equals(that.Topic) &&
                headers.Equals(that.headers);
        }

        public override string ToString()
        {
            return "ProcessorRecordContext{" +
                "topic='" + Topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", timestamp=" + timestamp +
                ", headers=" + headers +
                '}';
        }

        public override int GetHashCode()
            => base.GetHashCode();
    }
}