using RocksDbSharp;
using System.Collections.Generic;

namespace Kafka.Streams.State.Interfaces
{
    public interface IBulkLoadingStore
    {
        void toggleDbForBulkLoading(bool prepareForBulkload);
        void addToBatch(KeyValuePair<byte[], byte[]> record, WriteBatch batch);
        void write(WriteBatch batch);
    }
}