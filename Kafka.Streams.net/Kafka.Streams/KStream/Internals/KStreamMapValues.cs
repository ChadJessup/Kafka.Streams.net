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
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamMapValues<K, V, V1> : IProcessorSupplier<K, V>
    {
        private readonly IValueMapperWithKey<K, V, V1> mapper;
        //private IValueMapperWithKey<K, V1, V> swappedMapper;

        public KStreamMapValues(IValueMapperWithKey<K, V, V1> mapper)
            => this.mapper = mapper;

        //public KStreamMapValues(Func<K, V1, V> swappedMapper)
        //    => this.swappedMapper = new ValueMapperWithKey<K, V1, V>(swappedMapper);

        public KStreamMapValues(Func<K, V, V1> mapper)
            => this.mapper = new ValueMapperWithKey<K, V, V1>(mapper);

        public IKeyValueProcessor<K, V> get()
            => new KStreamMapProcessor<K, V>();

        //public static implicit operator KStreamMapValues<K, V1, V>(KStreamMapValues<K, V, V1> toSwap)
        //    => new KStreamMapValues<K, V1, V>(toSwap.swappedMapper);
    }
}
