/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Kafka.Common.Utils;
using Kafka.Streams.Errors;
using Kafka.Streams.State.KeyValues;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.State.RocksDbState
{
    public class RocksDbIterator : AbstractIterator<KeyValue<Bytes, byte[]>>
       , IKeyValueIterator<Bytes, byte[]>
    {
        private readonly string storeName;
        private readonly Iterator iter;
        private readonly HashSet<IKeyValueIterator<Bytes, byte[]>> openIterators;

        private volatile bool open = true;

        private KeyValue<Bytes, byte[]> next;

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
            if (!iter.Valid())
            {
                return allDone();
            }
            else
            {
                next = getKeyValue();
                iter.Next();
                return next;
            }
        }

        private KeyValue<Bytes, byte[]> getKeyValue()
        {
            return new KeyValue<Bytes, byte[]>(new Bytes(iter.Key()), iter.Value());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void close()
        {
            openIterators.Remove(this);
            //iter.close();
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