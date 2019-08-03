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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.streams.kstream.Suppressed;

import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.suppress.BufferFullStrategy.SHUT_DOWN;

public class StrictBufferConfigImpl : BufferConfigInternal<Suppressed.StrictBufferConfig> implements Suppressed.StrictBufferConfig {

    private  long maxRecords;
    private  long maxBytes;
    private  BufferFullStrategy bufferFullStrategy;

    public StrictBufferConfigImpl( long maxRecords,
                                   long maxBytes,
                                   BufferFullStrategy bufferFullStrategy) {
        this.maxRecords = maxRecords;
        this.maxBytes = maxBytes;
        this.bufferFullStrategy = bufferFullStrategy;
    }

    public StrictBufferConfigImpl() {
        this.maxRecords = Long.MAX_VALUE;
        this.maxBytes = Long.MAX_VALUE;
        this.bufferFullStrategy = SHUT_DOWN;
    }

    @Override
    public Suppressed.StrictBufferConfig withMaxRecords( long recordLimit) {
        return new StrictBufferConfigImpl(recordLimit, maxBytes, bufferFullStrategy);
    }

    @Override
    public Suppressed.StrictBufferConfig withMaxBytes( long byteLimit) {
        return new StrictBufferConfigImpl(maxRecords, byteLimit, bufferFullStrategy);
    }

    @Override
    public long maxRecords() {
        return maxRecords;
    }

    @Override
    public long maxBytes() {
        return maxBytes;
    }

    @Override
    public BufferFullStrategy bufferFullStrategy() {
        return bufferFullStrategy;
    }

    @Override
    public bool equals( Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
         StrictBufferConfigImpl that = (StrictBufferConfigImpl) o;
        return maxRecords == that.maxRecords &&
            maxBytes == that.maxBytes &&
            bufferFullStrategy == that.bufferFullStrategy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxRecords, maxBytes, bufferFullStrategy);
    }

    @Override
    public string toString() {
        return "StrictBufferConfigImpl{maxKeys=" + maxRecords +
            ", maxBytes=" + maxBytes +
            ", bufferFullStrategy=" + bufferFullStrategy + '}';
    }
}
