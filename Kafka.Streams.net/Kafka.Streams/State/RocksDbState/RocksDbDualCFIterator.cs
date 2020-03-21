using Kafka.Common.Utils;
using Kafka.Streams.Errors;
using Kafka.Streams.Internals;
using Kafka.Streams.State.KeyValues;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.State.RocksDbState
{
    public class RocksDbDualCFIterator : AbstractIterator<KeyValuePair<Bytes, byte[]>>
        , IKeyValueIterator<Bytes, byte[]>
    {
        // RocksDb's JNI interface does not expose getters/setters that allow the
        // comparator to be pluggable, and the default is lexicographic, so it's
        // safe to just force lexicographic comparator here for now.
        private readonly IComparer<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;

        private readonly string storeName;
        private readonly Iterator iterWithTimestamp;
        private readonly Iterator iterNoTimestamp;

        private volatile bool open = true;

        private byte[]? nextWithTimestamp;
        private byte[] nextNoTimestamp;
        private KeyValuePair<Bytes, byte[]> next;

        public RocksDbDualCFIterator(
            string storeName,
            Iterator iterWithTimestamp,
            Iterator iterNoTimestamp)
        {
            this.iterWithTimestamp = iterWithTimestamp;
            this.iterNoTimestamp = iterNoTimestamp;
            this.storeName = storeName;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public override bool hasNext()
        {
            if (!open)
            {
                throw new InvalidStateStoreException(string.Format("RocksDb iterator for store %s has closed", storeName));
            }

            return base.hasNext();
        }

        public override KeyValuePair<Bytes, byte[]> makeNext()
        {
            if (nextNoTimestamp == null && iterNoTimestamp.Valid())
            {
                nextNoTimestamp = iterNoTimestamp.Key();
            }

            if (nextWithTimestamp == null && iterWithTimestamp.Valid())
            {
                nextWithTimestamp = iterWithTimestamp.Key();
            }

            if (nextNoTimestamp == null && !iterNoTimestamp.Valid())
            {
                if (nextWithTimestamp == null && !iterWithTimestamp.Valid())
                {
                    return allDone();
                }
                else
                {
                    next = KeyValuePair.Create(new Bytes(nextWithTimestamp), iterWithTimestamp.Value());
                    nextWithTimestamp = null;
                    iterWithTimestamp.Next();
                }
            }
            else
            {
                if (nextWithTimestamp == null)
                {
                    next = KeyValuePair.Create(new Bytes(nextNoTimestamp), ApiUtils.convertToTimestampedFormat(iterNoTimestamp.Value()));
                    nextNoTimestamp = null;
                    iterNoTimestamp.Next();
                }
                else
                {
                    if (comparator.Compare(nextNoTimestamp, nextWithTimestamp) <= 0)
                    {
                        next = KeyValuePair.Create(new Bytes(nextNoTimestamp), ApiUtils.convertToTimestampedFormat(iterNoTimestamp.Value()));
                        nextNoTimestamp = null;
                        iterNoTimestamp.Next();
                    }
                    else
                    {
                        next = KeyValuePair.Create(new Bytes(nextWithTimestamp), iterWithTimestamp.Value());
                        nextWithTimestamp = null;
                        iterWithTimestamp.Next();
                    }
                }
            }

            return next;
        }

        public void Remove()
        {
            throw new InvalidOperationException("RocksDb iterator does not support Remove()");
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void close()
        {
            //openIterators.Remove(this);
            //iterNoTimestamp.close();
            //iterWithTimestamp.close();
            open = false;
        }

        public Bytes peekNextKey()
        {
            if (!hasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return next.Key;
        }
    }
}
