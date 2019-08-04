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
namespace Kafka.Streams.State.Internals;

using Kafka.Common.serialization.Serdes;
using Kafka.Common.Utils.Bytes;
using Kafka.Streams.kstream.Windowed;
using Kafka.Streams.Processor.internals.ProcessorStateManager;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.ISessionStore;
using Kafka.Streams.State.StateSerdes;

/**
 * Simple wrapper around a {@link ISessionStore} to support writing
 * updates to a changelog
 */
class ChangeLoggingSessionBytesStore
    : WrappedStateStore<ISessionStore<Bytes, byte[]>, byte[], byte[]>
    : ISessionStore<Bytes, byte[]>
{

    private StoreChangeLogger<Bytes, byte[]> changeLogger;

    ChangeLoggingSessionBytesStore(ISessionStore<Bytes, byte[]> bytesStore)
{
        base(bytesStore);
    }

    public override void init(IProcessorContext context, IStateStore root)
{
        base.init(context, root);
        string topic = ProcessorStateManager.storeChangelogTopic(
                context.applicationId(),
                name());
        changeLogger = new StoreChangeLogger<>(
                name(),
                context,
                new StateSerdes<>(topic, Serdes.Bytes(), Serdes.ByteArray()));
    }


    public override KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes key, long earliestSessionEndTime, long latestSessionStartTime)
{
        return wrapped().findSessions(key, earliestSessionEndTime, latestSessionStartTime);
    }

    public override KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes keyFrom, Bytes keyTo, long earliestSessionEndTime, long latestSessionStartTime)
{
        return wrapped().findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
    }

    public override void Remove(Windowed<Bytes> sessionKey)
{
        wrapped().Remove(sessionKey);
        changeLogger.logChange(SessionKeySchema.toBinary(sessionKey), null);
    }

    public override void put(Windowed<Bytes> sessionKey, byte[] aggregate)
{
        wrapped().Add(sessionKey, aggregate);
        changeLogger.logChange(SessionKeySchema.toBinary(sessionKey), aggregate);

    }

    public override byte[] fetchSession(Bytes key, long startTime, long endTime)
{
        return wrapped().fetchSession(key, startTime, endTime);
    }

    public override KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes key)
{
        return wrapped().fetch(key);
    }

    public override KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from, Bytes to)
{
        return wrapped().fetch(from, to);
    }
}
