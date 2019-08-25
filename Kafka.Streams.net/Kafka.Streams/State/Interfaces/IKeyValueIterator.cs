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
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Interfaces
{
    /**
     * IEnumerator interface of {@link KeyValue}.
     *
     * Users must call its {@code close} method explicitly upon completeness to release resources,
     * or use try-with-resources statement (available since JDK7) for this {@link IDisposable}.
     *
     * @param Type of keys
     * @param Type of values
     */
    public interface IKeyValueIterator<K, V> : IEnumerator<KeyValue<K, V>>, IDisposable
    {
        abstract void close();

        /**
         * Peek at the next key without advancing the iterator
         * @return the key of the next value that would be returned from the next call to next
         */
        K peekNextKey();
    }
}