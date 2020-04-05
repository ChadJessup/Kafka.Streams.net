using Kafka.Streams.State.KeyValues;
using RocksDbSharp;
using System.Collections.Generic;

namespace Kafka.Streams.State.RocksDbState
{
    public interface IRocksDbAccessor
    {
        void Put(byte[] key, byte[] value);

        void PrepareBatch(List<KeyValuePair<Bytes, byte[]>> entries,
                          WriteBatch batch);

        byte[] Get(byte[] key);

        /**
         * In contrast to get(), we don't migrate the key to new CF.
         * <p>
         * Use for get() within delete() -- no need to migrate, as it's deleted anyway
         */
        byte[] GetOnly(byte[] key);

        IKeyValueIterator<Bytes, byte[]> Range(
            Bytes from,
            Bytes to);

        IKeyValueIterator<Bytes, byte[]> All();

        long ApproximateNumEntries();

        void Flush();

        void PrepareBatchForRestore(
            List<KeyValuePair<byte[], byte[]>> records,
            WriteBatch batch);

        void AddToBatch(
            byte[] key,
            byte[] value,
            WriteBatch batch);

        void Close();

        void ToggleDbForBulkLoading();
    }
}