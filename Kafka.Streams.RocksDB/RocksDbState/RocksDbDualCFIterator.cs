using Kafka.Common.Utils;
using Kafka.Streams.Errors;
using Kafka.Streams.Internals;
using Kafka.Streams.State.KeyValues;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.RocksDbState
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
        public override bool HasNext()
        {
            if (!this.open)
            {
                throw new InvalidStateStoreException(string.Format("RocksDb iterator for store %s has closed", this.storeName));
            }

            return base.HasNext();
        }

        public override KeyValuePair<Bytes, byte[]> MakeNext()
        {
            if (this.nextNoTimestamp == null && this.iterNoTimestamp.Valid())
            {
                this.nextNoTimestamp = this.iterNoTimestamp.Key();
            }

            if (this.nextWithTimestamp == null && this.iterWithTimestamp.Valid())
            {
                this.nextWithTimestamp = this.iterWithTimestamp.Key();
            }

            if (this.nextNoTimestamp == null && !this.iterNoTimestamp.Valid())
            {
                if (this.nextWithTimestamp == null && !this.iterWithTimestamp.Valid())
                {
                    return this.allDone();
                }
                else
                {
                    this.next = KeyValuePair.Create(new Bytes(this.nextWithTimestamp), this.iterWithTimestamp.Value());
                    this.nextWithTimestamp = null;
                    this.iterWithTimestamp.Next();
                }
            }
            else
            {
                if (this.nextWithTimestamp == null)
                {
                    this.next = KeyValuePair.Create(new Bytes(this.nextNoTimestamp), ApiUtils.ConvertToTimestampedFormat(this.iterNoTimestamp.Value()));
                    this.nextNoTimestamp = null;
                    this.iterNoTimestamp.Next();
                }
                else
                {
                    if (this.comparator.Compare(this.nextNoTimestamp, this.nextWithTimestamp) <= 0)
                    {
                        this.next = KeyValuePair.Create(new Bytes(this.nextNoTimestamp), ApiUtils.ConvertToTimestampedFormat(this.iterNoTimestamp.Value()));
                        this.nextNoTimestamp = null;
                        this.iterNoTimestamp.Next();
                    }
                    else
                    {
                        this.next = KeyValuePair.Create(new Bytes(this.nextWithTimestamp), this.iterWithTimestamp.Value());
                        this.nextWithTimestamp = null;
                        this.iterWithTimestamp.Next();
                    }
                }
            }

            return this.next;
        }

        public void Remove()
        {
            throw new InvalidOperationException("RocksDb iterator does not support Remove()");
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Close()
        {
            //openIterators.Remove(this);
            //iterNoTimestamp.Close();
            //iterWithTimestamp.Close();
            this.open = false;
        }

        public Bytes PeekNextKey()
        {
            if (!this.HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return this.next.Key;
        }
    }
}
