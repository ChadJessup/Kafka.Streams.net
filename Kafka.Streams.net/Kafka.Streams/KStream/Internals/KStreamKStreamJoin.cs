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
using Kafka.Streams.Processor;
using Microsoft.Extensions.Logging;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamKStreamJoin<K, R, V1, V2> : IProcessorSupplier<K, V1>
    {
        private static readonly ILogger LOG = new LoggerFactory().CreateLogger<KStreamKStreamJoin<K, R, V1, V2>>();

        private readonly string otherWindowName;
        private readonly TimeSpan joinBeforeMs;
        private readonly TimeSpan joinAfterMs;

        private readonly IValueJoiner<V1, V2, R> joiner;
        private readonly bool outer;

        public KStreamKStreamJoin(
            string otherWindowName,
            TimeSpan joinBeforeMs,
            TimeSpan joinAfterMs,
            IValueJoiner<V1, V2, R> joiner,
            bool outer)
        {
            this.otherWindowName = otherWindowName;
            this.joinBeforeMs = joinBeforeMs;
            this.joinAfterMs = joinAfterMs;
            this.joiner = joiner;
            this.outer = outer;
        }

        public IProcessor<K, V1> get()
        {
            return null;// new KStreamKStreamJoinProcessor();
        }
    }
}