using Kafka.Common.Utils;
using RocksDbSharp;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
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
            iterWithTimestamp.Seek(from.get());
            iterNoTimestamp.Seek(from.get());
            upperBoundKey = to.get();
            if (upperBoundKey == null)
            {
                throw new ArgumentNullException("RocksDbDualCFRangeIterator: upperBoundKey is null for key " + to);
            }
        }


        public override KeyValue<Bytes, byte[]> makeNext()
        {
            KeyValue<Bytes, byte[]> next = base.makeNext();

            if (next == null)
            {
                return allDone();
            }
            else
            {
                if (comparator.Compare(next.key.get(), upperBoundKey) <= 0)
                {
                    return next;
                }
                else
                {
                    return allDone();
                }
            }
        }
    }
}
