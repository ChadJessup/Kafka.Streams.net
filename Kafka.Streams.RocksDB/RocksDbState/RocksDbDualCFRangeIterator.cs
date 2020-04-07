using RocksDbSharp;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.RocksDbState
{
    public class RocksDbDualCFRangeIterator : RocksDbDualCFIterator
    {
        // RocksDb's JNI interface does not expose getters/setters that allow the
        // comparator to be pluggable, and the default is lexicographic, so it's
        // safe to just force lexicographic comparator here for now.
        private readonly IComparer<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
        private readonly byte[] upperBoundKey;

        public RocksDbDualCFRangeIterator(
            string storeName,
            Iterator iterWithTimestamp,
            Iterator iterNoTimestamp,
            Bytes from,
            Bytes to)
            : base(storeName, iterWithTimestamp, iterNoTimestamp)
        {
            iterWithTimestamp.Seek(from.Get());
            iterNoTimestamp.Seek(from.Get());
            upperBoundKey = to.Get();
            if (upperBoundKey == null)
            {
                throw new ArgumentNullException("RocksDbDualCFRangeIterator: upperBoundKey is null for key " + to);
            }
        }


        public override KeyValuePair<Bytes, byte[]> MakeNext()
        {
            KeyValuePair<Bytes, byte[]>? next = base.MakeNext();

            if (next == null)
            {
                return allDone();
            }
            else
            {
                if (comparator.Compare(next.Value.Key.Get(), upperBoundKey) <= 0)
                {
                    return next.Value;
                }
                else
                {
                    return allDone();
                }
            }
        }
    }
}
