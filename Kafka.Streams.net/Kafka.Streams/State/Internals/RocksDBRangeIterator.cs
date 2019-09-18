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
using Kafka.Streams.State.Interfaces;
using RocksDbSharp;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
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
                if (comparator.Compare(next.key.get(), rawToKey) <= 0)
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