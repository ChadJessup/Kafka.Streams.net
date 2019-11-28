using Kafka.Common.Utils;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValue;
using RocksDbSharp;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.RocksDbState
{
    public class RocksDbRangeIterator : RocksDbIterator
    {
        // RocksDb's JNI interface does not expose getters/setters that allow the
        // comparator to be pluggable, and the default is lexicographic, so it's
        // safe to just force lexicographic comparator here for now.
        private readonly LexicographicByteArrayComparator comparator = Bytes.BYTES_LEXICO_COMPARATOR;
        private readonly byte[] rawToKey;

        public RocksDbRangeIterator(
            string storeName,
            Iterator iter,
            HashSet<IKeyValueIterator<Bytes, byte[]>> openIterators,
            Bytes from,
            Bytes to)
            : base(storeName, iter, openIterators)
        {
            iter.Seek(from.get());

            rawToKey = to.get();
            if (rawToKey == null)
            {
                throw new ArgumentNullException("RocksDbRangeIterator: RawToKey is null for key " + to);
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
                if (comparator.Compare(next.Key.get(), rawToKey) <= 0)
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