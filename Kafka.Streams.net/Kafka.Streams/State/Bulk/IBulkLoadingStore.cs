using RocksDbSharp;
using System.Collections.Generic;

namespace Kafka.Streams.State.Interfaces
{
    public interface IBulkLoadingStore
    {
        void ToggleDbForBulkLoading(bool prepareForBulkload);
        void AddToBatch(KeyValuePair<byte[], byte[]> record, WriteBatch batch);
        void Write(WriteBatch batch);
    }
}