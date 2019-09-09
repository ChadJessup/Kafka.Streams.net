using Confluent.Kafka;
using Kafka.Streams.KStream.Internals;

namespace Kafka.Streams.State.Interfaces
{
    public interface IRecordConverter
    {
        ConsumeResult<byte[], byte[]> convert(ConsumeResult<byte[], byte[]> record);
    }

    public class IdentityRecordConverter : IRecordConverter
    {
        public ConsumeResult<byte[], byte[]> convert(ConsumeResult<byte[], byte[]> record)
            => record;
    }

    public class RawToTimeStampInstance : IRecordConverter
    {
        public ConsumeResult<byte[], byte[]> convert(ConsumeResult<byte[], byte[]> record)
        {
            byte[] rawValue = record.Value;
            Timestamp timestamp = record.Timestamp;
            byte[]? recordValue = rawValue == null
                ? null
                : new ByteBuffer().allocate(8 + rawValue.Length)
                    .putLong(timestamp.UnixTimestampMs)
                    .Add(rawValue)
                    .array();

            return new ConsumeResult<byte[], byte[]>()
            {
                Topic = record.Topic,
                Partition = record.Partition,
                Offset = record.Offset,
                Timestamp = timestamp,
                //record.timestampType(),
                //record.checksum(),
                //record.serializedKeySize(),
                //record.serializedValueSize(),
                Key = record.Key,
                Value = recordValue,
                Headers = record.Headers,
                IsPartitionEOF = record.IsPartitionEOF,
                Message = record.Message,
                //TopicPartition = record.TopicPartition,
                TopicPartitionOffset = record.TopicPartitionOffset,
                //record.leaderEpoch());
            };
        }
    }
}
