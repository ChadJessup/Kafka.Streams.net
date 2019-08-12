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
using Kafka.Streams.State.Interfaces;
using Kafka.Common.Utils;
using Kafka.Streams.KStream;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals;

namespace Kafka.Streams.State.Internals
{
    /**
     * Simple wrapper around a {@link ISessionStore} to support writing
     * updates to a changelog
     */
    public class ChangeLoggingSessionBytesStore
        : WrappedStateStore<ISessionStore<Bytes, byte[]>, byte[], byte[]>,
        ISessionStore<Bytes, byte[]>
    {
        private StoreChangeLogger<Bytes, byte[]> changeLogger;

        public ChangeLoggingSessionBytesStore(ISessionStore<Bytes, byte[]> bytesStore)
            : base(bytesStore)
        {
        }

        public override void init(IProcessorContext<Bytes, byte[]> context, IStateStore root)
        {
            base.init(context, root);
            string topic = ProcessorStateManager.storeChangelogTopic(
                    context.applicationId(),
                    name);

            changeLogger = new StoreChangeLogger<>(
                    name,
                    context,
                    new StateSerdes<>(topic, Serdes.Bytes(), Serdes.ByteArray()));
        }


        public override IKeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes key, long earliestSessionEndTime, long latestSessionStartTime)
        {
            return wrapped.findSessions(key, earliestSessionEndTime, latestSessionStartTime);
        }

        public override IKeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes keyFrom, Bytes keyTo, long earliestSessionEndTime, long latestSessionStartTime)
        {
            return wrapped.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
        }

        public override void Remove(Windowed<Bytes> sessionKey)
        {
            wrapped.Remove(sessionKey);
            changeLogger.logChange(SessionKeySchema.toBinary(sessionKey), null);
        }

        public override void put(Windowed<Bytes> sessionKey, byte[] aggregate)
        {
            wrapped.Add(sessionKey, aggregate);
            changeLogger.logChange(SessionKeySchema.toBinary(sessionKey), aggregate);

        }

        public override byte[] fetchSession(Bytes key, long startTime, long endTime)
        {
            return wrapped.fetchSession(key, startTime, endTime);
        }

        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes key)
        {
            return wrapped.fetch(key);
        }

        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from, Bytes to)
        {
            return wrapped.fetch(from, to);
        }
    }
}