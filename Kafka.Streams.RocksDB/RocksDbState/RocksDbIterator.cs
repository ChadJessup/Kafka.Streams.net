using Kafka.Common.Utils;
using Kafka.Streams.Errors;
using Kafka.Streams.State.KeyValues;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.RocksDbState
{
    public class RocksDbIterator : AbstractIterator<KeyValuePair<Bytes, byte[]>>
       , IKeyValueIterator<Bytes, byte[]>
    {
        private readonly string storeName;
        private readonly Iterator iter;
        private readonly HashSet<IKeyValueIterator<Bytes, byte[]>> openIterators;

        private volatile bool open = true;

        private KeyValuePair<Bytes, byte[]> _next;

        public RocksDbIterator(
            string storeName,
            Iterator iter,
            HashSet<IKeyValueIterator<Bytes, byte[]>> openIterators)
        {
            this.storeName = storeName;
            this.iter = iter;
            this.openIterators = openIterators;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public override bool HasNext()
        {
            if (!this.open)
            {
                throw new InvalidStateStoreException(string.Format("RocksDb iterator for store %s has closed", this.storeName));
            }
            return base.MoveNext();
        }

        public override KeyValuePair<Bytes, byte[]> MakeNext()
        {
            if (!this.iter.Valid())
            {
                return this.allDone();
            }
            else
            {
                this._next = this.GetKeyValue();
                this.iter.Next();
                return this._next;
            }
        }

        private KeyValuePair<Bytes, byte[]> GetKeyValue()
        {
            return new KeyValuePair<Bytes, byte[]>(new Bytes(this.iter.Key()), this.iter.Value());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Close()
        {
            this.openIterators.Remove(this);
            //iter.Close();
            this.open = false;
        }

        public Bytes PeekNextKey()
        {
            if (!this.MoveNext())
            {
                throw new IndexOutOfRangeException();
            }

            return this._next.Key;
        }
    }
}