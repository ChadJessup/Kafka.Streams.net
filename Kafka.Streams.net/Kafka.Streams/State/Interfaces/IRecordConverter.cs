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
                IsPartitionEOF = record.IsPartitionEOF,
                Message = record.Message,
                TopicPartitionOffset = record.TopicPartitionOffset,
            };
        }
    }
}
