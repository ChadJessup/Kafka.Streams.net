/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.streams.state;

using Kafka.Streams.kstream.Windowed;

/**
 * Interface for storing the aggregated values of fixed-size time windows.
 * <p>
 * Note, that the stores's physical key type is {@link Windowed Windowed&lt;K&gt;}.
 * In contrast to a {@link WindowStore} that stores plain windowedKeys-value pairs,
 * a {@code TimestampedWindowStore} stores windowedKeys-(value/timestamp) pairs.
 * <p>
 * While the window start- and end-timestamp are fixed per window, the value-side timestamp is used
 * to store the last update timestamp of the corresponding window.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
public interface TimestampedWindowStore<K, V> : WindowStore<K, ValueAndTimestamp<V>> { }
