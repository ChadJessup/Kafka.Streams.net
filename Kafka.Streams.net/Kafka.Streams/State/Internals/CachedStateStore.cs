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

public interface CachedStateStore<K, V>
{
    /**
     * Set the {@link CacheFlushListener} to be notified when entries are flushed from the
     * cache to the underlying {@link org.apache.kafka.streams.processor.IStateStore}
     * @param listener
     * @param sendOldValues
     */
    bool setFlushListener(CacheFlushListener<K, V> listener,
                             bool sendOldValues);
}
