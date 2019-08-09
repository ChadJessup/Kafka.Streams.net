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
namespace Kafka.Streams.KStream.Internals
{









/**
 * This is used to determine if a processor should forward values to child nodes.
 * Forwarding by this only occurs when caching is not enabled. If caching is enabled,
 * forwarding occurs in the flush listener when the cached store flushes.
 *
 * @param <K>
 * @param <V>
 */
class SessionTupleForwarder<K, V> {
    private  IProcessorContext<K, V> context;
    private  bool sendOldValues;
    private  bool cachingEnabled;

    
    SessionTupleForwarder( IStateStore store,
                           IProcessorContext<K, V> context,
                           CacheFlushListener<Windowed<K>, V> flushListener,
                           bool sendOldValues)
{
        this.context = context;
        this.sendOldValues = sendOldValues;
        cachingEnabled = ((WrappedStateStore) store).setFlushListener(flushListener, sendOldValues);
    }

    public void maybeForward( Windowed<K> key,
                              V newValue,
                              V oldValue)
{
        if (!cachingEnabled)
{
            context.forward(key, new Change<>(newValue, sendOldValues ? oldValue : null), To.all().withTimestamp(key.window().end()));
        }
    }
}
