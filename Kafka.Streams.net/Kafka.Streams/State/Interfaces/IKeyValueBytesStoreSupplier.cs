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
using Kafka.Streams.State.Internals;

namespace Kafka.Streams.State
{
    /**
     * A store supplier that can be used to create one or more {@link KeyValueStore KeyValueStore&lt;Byte, byte[]&gt;} instances of type &lt;Byte, byte[]&gt;.
     *
     * For any stores implementing the {@link KeyValueStore KeyValueStore&lt;Byte, byte[]&gt;} interface, null value bytes are considered as "not exist". This means:
     *
     * 1. Null value bytes in put operations should be treated as delete.
     * 2. If the key does not exist, get operations should return null value bytes.
     */
    public interface IKeyValueBytesStoreSupplier : IStoreSupplier<IKeyValueStore<Bytes, byte[]>>
    {

    }
}