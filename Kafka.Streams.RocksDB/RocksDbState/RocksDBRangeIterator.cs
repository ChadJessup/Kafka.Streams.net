using Kafka.Common.Utils;
using Kafka.Streams.State.KeyValues;
using RocksDbSharp;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.RocksDbState
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
            iter.Seek(from.Get());

            this.rawToKey = to.Get();
            if (this.rawToKey == null)
            {
                throw new ArgumentNullException("RocksDbRangeIterator: RawToKey is null for key " + to);
            }
        }

        public override KeyValuePair<Bytes, byte[]> MakeNext()
        {
            KeyValuePair<Bytes, byte[]>? next = base.MakeNext();

            if (next == null)
            {
                return this.allDone();
            }
            else
            {
                if (this.comparator.Compare(next.Value.Key.Get(), this.rawToKey) <= 0)
                {
                    return next.Value;
                }
                else
                {
                    return this.allDone();
                }
            }
        }
    }
}