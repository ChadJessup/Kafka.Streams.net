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

        private KeyValuePair<Bytes, byte[]> next;

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
            if (!open)
            {
                throw new InvalidStateStoreException(string.Format("RocksDb iterator for store %s has closed", storeName));
            }
            return base.HasNext();
        }

        public override KeyValuePair<Bytes, byte[]> MakeNext()
        {
            if (!iter.Valid())
            {
                return allDone();
            }
            else
            {
                next = GetKeyValue();
                iter.Next();
                return next;
            }
        }

        private KeyValuePair<Bytes, byte[]> GetKeyValue()
        {
            return new KeyValuePair<Bytes, byte[]>(new Bytes(iter.Key()), iter.Value());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Close()
        {
            openIterators.Remove(this);
            //iter.close();
            open = false;
        }

        public Bytes PeekNextKey()
        {
            if (!HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return next.Key;
        }
    }
}