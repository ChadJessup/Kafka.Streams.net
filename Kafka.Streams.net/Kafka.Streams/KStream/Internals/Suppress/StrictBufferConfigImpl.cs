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
    public class StrictBufferConfigImpl : BufferConfigInternal<Suppressed.StrictBufferConfig>, Suppressed.StrictBufferConfig
    {
        private long maxRecords;
        private long maxBytes;
        private BufferFullStrategy bufferFullStrategy;

        public StrictBufferConfigImpl(long maxRecords,
                                       long maxBytes,
                                       BufferFullStrategy bufferFullStrategy)
        {
            this.maxRecords = maxRecords;
            this.maxBytes = maxBytes;
            this.bufferFullStrategy = bufferFullStrategy;
        }

        public StrictBufferConfigImpl()
        {
            this.maxRecords = long.MaxValue;
            this.maxBytes = long.MaxValue;
            this.bufferFullStrategy = SHUT_DOWN;
        }


        public Suppressed.StrictBufferConfig withMaxRecords(long recordLimit)
        {
            return new StrictBufferConfigImpl(recordLimit, maxBytes, bufferFullStrategy);
        }


        public Suppressed.StrictBufferConfig withMaxBytes(long byteLimit)
        {
            return new StrictBufferConfigImpl(maxRecords, byteLimit, bufferFullStrategy);
        }


        public long maxRecords()
        {
            return maxRecords;
        }


        public long maxBytes()
        {
            return maxBytes;
        }


        public BufferFullStrategy bufferFullStrategy()
        {
            return bufferFullStrategy;
        }


        public bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || GetType() != o.GetType())
            {
                return false;
            }
            StrictBufferConfigImpl that = (StrictBufferConfigImpl)o;
            return maxRecords == that.maxRecords &&
                maxBytes == that.maxBytes &&
                bufferFullStrategy == that.bufferFullStrategy;
        }


        public int hashCode()
        {
            return Objects.hash(maxRecords, maxBytes, bufferFullStrategy);
        }


        public string ToString()
        {
            return "StrictBufferConfigImpl{maxKeys=" + maxRecords +
                ", maxBytes=" + maxBytes +
                ", bufferFullStrategy=" + bufferFullStrategy + '}';
        }
    }
}