using Kafka.Common.Utils;
using Kafka.Streams.Errors;
using Kafka.Streams.Internals;
using Kafka.Streams.State.Interfaces;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.State.Internals
{
    public class RocksDbDualCFIterator : AbstractIterator<KeyValue<Bytes, byte[]>>
        , IKeyValueIterator<Bytes, byte[]>
    {
        // RocksDb's JNI interface does not expose getters/setters that allow the
        // comparator to be pluggable, and the default is lexicographic, so it's
        // safe to just force lexicographic comparator here for now.
        private IComparer<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;

        private string storeName;
        private Iterator iterWithTimestamp;
        private Iterator iterNoTimestamp;

        private volatile bool open = true;

        private byte[] nextWithTimestamp;
        private byte[] nextNoTimestamp;
        private KeyValue<Bytes, byte[]> next;

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

        public override KeyValue<Bytes, byte[]> makeNext()
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
                    next = KeyValue<Bytes, byte[]>.pair(new Bytes(nextWithTimestamp), iterWithTimestamp.Value());
                    nextWithTimestamp = null;
                    iterWithTimestamp.Next();
                }
            }
            else
            {
                if (nextWithTimestamp == null)
                {
                    next = KeyValue<Bytes, byte[]>.pair(new Bytes(nextNoTimestamp), ApiUtils.convertToTimestampedFormat(iterNoTimestamp.Value()));
                    nextNoTimestamp = null;
                    iterNoTimestamp.Next();
                }
                else
                {
                    if (comparator.Compare(nextNoTimestamp, nextWithTimestamp) <= 0)
                    {
                        next = KeyValue<Bytes, byte[]>.pair(new Bytes(nextNoTimestamp), ApiUtils.convertToTimestampedFormat(iterNoTimestamp.Value()));
                        nextNoTimestamp = null;
                        iterNoTimestamp.Next();
                    }
                    else
                    {
                        next = KeyValue<Bytes, byte[]>.pair(new Bytes(nextWithTimestamp), iterWithTimestamp.Value());
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

            return next.key;
        }
    }
}
