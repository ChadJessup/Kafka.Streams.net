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
namespace Kafka.Streams.State.Internals
{
    public class InMemoryWindowBytesStoreSupplier : IWindowBytesStoreSupplier
    {
        private string name;
        private long retentionPeriod;
        private long windowSize;
        private bool retainDuplicates;

        public InMemoryWindowBytesStoreSupplier(string name,
                                                long retentionPeriod,
                                                long windowSize,
                                                bool retainDuplicates)
        {
            this.name = name;
            this.retentionPeriod = retentionPeriod;
            this.windowSize = windowSize;
            this.retainDuplicates = retainDuplicates;
        }

        public override string name()
        {
            return name;
        }

        public override IWindowStore<Bytes, byte[]> get()
        {
            return new InMemoryWindowStore(name,
                                           retentionPeriod,
                                           windowSize,
                                           retainDuplicates,
                                           metricsScope());
        }

        public override string metricsScope()
        {
            return "in-memory-window-state";
        }

        [System.Obsolete]
        public override int segments()
        {
            throw new InvalidOperationException("Segments is deprecated and should not be called");
        }

        public override long retentionPeriod()
        {
            return retentionPeriod;
        }


        public override long windowSize()
        {
            return windowSize;
        }

        // In-memory window store is not *really* segmented, so just say size is 1 ms
        public override long segmentIntervalMs()
        {
            return 1;
        }

        public override bool retainDuplicates()
        {
            return retainDuplicates;
        }
    }
}