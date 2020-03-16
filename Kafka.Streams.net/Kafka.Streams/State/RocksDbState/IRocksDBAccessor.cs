using Kafka.Streams.State.KeyValues;
using RocksDbSharp;
using System.Collections.Generic;

namespace Kafka.Streams.State.RocksDbState
{
    public interface IRocksDbAccessor
    {
        void put(byte[] key, byte[] value);

        void prepareBatch(List<KeyValue<Bytes, byte[]>> entries,
                          WriteBatch batch);

        byte[] get(byte[] key);

        /**
         * In contrast to get(), we don't migrate the key to new CF.
         * <p>
         * Use for get() within delete() -- no need to migrate, as it's deleted anyway
         */
        byte[] getOnly(byte[] key);

        IKeyValueIterator<Bytes, byte[]> range(
            Bytes from,
            Bytes to);

        IKeyValueIterator<Bytes, byte[]> all();

        long approximateNumEntries();

        void flush();

        void prepareBatchForRestore(
            List<KeyValue<byte[], byte[]>> records,
            WriteBatch batch);

        void addToBatch(
            byte[] key,
            byte[] value,
            WriteBatch batch);

        void close();

        void toggleDbForBulkLoading();
    }
}