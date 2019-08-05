using RocksDbSharp;

namespace Kafka.Streams.State.Interfaces
{
    public interface IBulkLoadingStore
    {
        void toggleDbForBulkLoading(bool prepareForBulkload);
        void addToBatch(KeyValue<byte[], byte[]> record, WriteBatch batch);
        void write(WriteBatch batch);
    }
}