using Confluent.Kafka;
using Kafka.Streams.KStream.Internals;

namespace Kafka.Streams.State.Interfaces
{
    public interface IRecordConverter
    {
        ConsumeResult<byte[], byte[]> Convert(ConsumeResult<byte[], byte[]> record);
    }

    public class IdentityRecordConverter : IRecordConverter
    {
        public ConsumeResult<byte[], byte[]> Convert(ConsumeResult<byte[], byte[]> record)
            => record;
    }

    public class RawToTimeStampInstance : IRecordConverter
    {
        public ConsumeResult<byte[], byte[]> Convert(ConsumeResult<byte[], byte[]> record)
        {
            var RawValue = record.Value;
            Timestamp timestamp = record.Timestamp;
            var recordValue = RawValue == null
                ? null
                : new ByteBuffer().Allocate(8 + RawValue.Length)
                    .PutLong(timestamp.UnixTimestampMs)
                    .Add(RawValue)
                    .Array();

            return new ConsumeResult<byte[], byte[]>()
            {
                IsPartitionEOF = record.IsPartitionEOF,
                Message = record.Message,
                TopicPartitionOffset = record.TopicPartitionOffset,
            };
        }
    }
}
