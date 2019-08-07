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
namespace Kafka.Streams.KStream.Internals.Suppress
{
    abstract class BufferConfigInternal<BC> : Suppressed.BufferConfig<BC>
        where BC : Suppressed.BufferConfig<BC>
    {
        public abstract long maxRecords();

        public abstract long maxBytes();


        public abstract BufferFullStrategy bufferFullStrategy();


        public Suppressed.StrictBufferConfig withNoBound()
        {
            return new StrictBufferConfigImpl(
                long.MaxValue,
                long.MaxValue,
                SHUT_DOWN // doesn't matter, given the bounds
            );
        }


        public Suppressed.StrictBufferConfig shutDownWhenFull()
        {
            return new StrictBufferConfigImpl(maxRecords(), maxBytes(), SHUT_DOWN);
        }


        public Suppressed.EagerBufferConfig emitEarlyWhenFull()
        {
            return new EagerBufferConfigImpl(maxRecords(), maxBytes());
        }
    }
}