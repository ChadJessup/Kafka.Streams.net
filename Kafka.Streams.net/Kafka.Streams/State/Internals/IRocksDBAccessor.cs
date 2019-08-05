using Kafka.Common.Utils;
using Kafka.Streams.State;
using RocksDbSharp;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public interface IRocksDBAccessor
    {

        void put(byte[] key,
                 byte[] value);

        void prepareBatch(List<KeyValue<Bytes, byte[]>> entries,
                          WriteBatch batch);

        byte[] get(byte[] key);

        /**
         * In contrast to get(), we don't migrate the key to new CF.
         * <p>
         * Use for get() within delete() -- no need to migrate, as it's deleted anyway
         */
        byte[] getOnly(byte[] key);

        KeyValueIterator<Bytes, byte[]> range(Bytes from,
                                              Bytes to);

        KeyValueIterator<Bytes, byte[]> all();

        long approximateNumEntries();

        void flush();

        void prepareBatchForRestore(Collection<KeyValue<byte[], byte[]>> records,
                                    WriteBatch batch);

        void addToBatch(byte[] key,
                        byte[] value,
                        WriteBatch batch);

        void close();

        void toggleDbForBulkLoading();
    }
}