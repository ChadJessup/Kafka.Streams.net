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
using Kafka.Streams.KStream.Internals.Suppress;

namespace Kafka.Streams.KStream
{
    public interface IBufferConfig<BC>
        where BC : IBufferConfig<BC>
    {
        /**
         * Create a size-constrained buffer in terms of the maximum number of keys it will store.
         */
        public IEagerBufferConfig maxRecords(long recordLimit)
        {
            return new EagerBufferConfigImpl(recordLimit, long.MaxValue);
        }

    /**
     * Set a size constraint on the buffer in terms of the maximum number of keys it will store.
     */
    BC withMaxRecords(long recordLimit);

    /**
     * Create a size-constrained buffer in terms of the maximum number of bytes it will use.
     */
    public IEagerBufferConfig maxBytes(long byteLimit)
    {
        return new EagerBufferConfigImpl(long.MaxValue, byteLimit);
    }

    /**
     * Set a size constraint on the buffer, the maximum number of bytes it will use.
     */
    BC withMaxBytes(long byteLimit);

    /**
     * Create a buffer unconstrained by size (either keys or bytes).
     *
     * As a result, the buffer will consume as much memory as it needs, dictated by the time bound.
     *
     * If there isn't enough heap available to meet the demand, the application will encounter an
     * {@link OutOfMemoryError} and shut down (not guaranteed to be a graceful exit). Also, note that
     * JVM processes under extreme memory pressure may exhibit poor GC behavior.
     *
     * This is a convenient option if you doubt that your buffer will be that large, but also don't
     * wish to pick particular constraints, such as in testing.
     *
     * This buffer is "strict" in the sense that it will enforce the time bound or crash.
     * It will never emit early.
     */
    public IStrictBufferConfig unbounded();
    //{
    //    return new StrictBufferConfigImpl();
    //}

    /**
     * Set the buffer to be unconstrained by size (either keys or bytes).
     *
     * As a result, the buffer will consume as much memory as it needs, dictated by the time bound.
     *
     * If there isn't enough heap available to meet the demand, the application will encounter an
     * {@link OutOfMemoryError} and shut down (not guaranteed to be a graceful exit). Also, note that
     * JVM processes under extreme memory pressure may exhibit poor GC behavior.
     *
     * This is a convenient option if you doubt that your buffer will be that large, but also don't
     * wish to pick particular constraints, such as in testing.
     *
     * This buffer is "strict" in the sense that it will enforce the time bound or crash.
     * It will never emit early.
     */
    IStrictBufferConfig withNoBound();

    /**
     * Set the buffer to gracefully shut down the application when any of its constraints are violated
     *
     * This buffer is "strict" in the sense that it will enforce the time bound or shut down.
     * It will never emit early.
     */
    IStrictBufferConfig shutDownWhenFull();

    /**
     * Set the buffer to just emit the oldest records when any of its constraints are violated.
     *
     * This buffer is "not strict" in the sense that it may emit early, so it is suitable for reducing
     * duplicate results downstream, but does not promise to eliminate them.
     */
    IEagerBufferConfig emitEarlyWhenFull();
}
}