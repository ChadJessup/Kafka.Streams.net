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

namespace Kafka.Streams.State.Internals
{
    public class RocksDBSessionStore
        : WrappedStateStore<SegmentedBytesStore, object, object>, ISessionStore<Bytes, byte[]>
    {
        public RocksDBSessionStore(SegmentedBytesStore bytesStore)
            : base(bytesStore)
        {
        }

        public override KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes key,
                                                                      long earliestSessionEndTime,
                                                                      long latestSessionStartTime)
        {
            KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(
                key,
                earliestSessionEndTime,
                latestSessionStartTime
            );
            return new WrappedSessionStoreIterator(bytesIterator);
        }

        public override KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes keyFrom,
                                                                      Bytes keyTo,
                                                                      long earliestSessionEndTime,
                                                                      long latestSessionStartTime)
        {
            KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(
                keyFrom,
                keyTo,
                earliestSessionEndTime,
                latestSessionStartTime
            );
            return new WrappedSessionStoreIterator(bytesIterator);
        }

        public override byte[] fetchSession(Bytes key, long startTime, long endTime)
        {
            return wrapped()[SessionKeySchema.toBinary(key, startTime, endTime));
        }

        public override KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes key)
        {
            return findSessions(key, 0, long.MaxValue);
        }

        public override KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from, Bytes to)
        {
            return findSessions(from, to, 0, long.MaxValue);
        }

        public override void Remove(Windowed<Bytes> key)
        {
            wrapped().Remove(SessionKeySchema.toBinary(key));
        }

        public override void put(Windowed<Bytes> sessionKey, byte[] aggregate)
        {
            wrapped().Add(SessionKeySchema.toBinary(sessionKey), aggregate);
        }
    }
}