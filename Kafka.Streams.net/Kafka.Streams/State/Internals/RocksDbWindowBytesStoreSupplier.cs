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
    public class RocksDbWindowBytesStoreSupplier : IWindowBytesStoreSupplier
    {
        private string name;
        private long retentionPeriod;
        private long segmentInterval;
        private long windowSize;
        private bool retainDuplicates;
        private bool returnTimestampedStore;

        public RocksDbWindowBytesStoreSupplier(string name,
                                               long retentionPeriod,
                                               long segmentInterval,
                                               long windowSize,
                                               bool retainDuplicates,
                                               bool returnTimestampedStore)
        {
            this.name = name;
            this.retentionPeriod = retentionPeriod;
            this.segmentInterval = segmentInterval;
            this.windowSize = windowSize;
            this.retainDuplicates = retainDuplicates;
            this.returnTimestampedStore = returnTimestampedStore;
        }

        public override string name()
        {
            return name;
        }

        public override IWindowStore<Bytes, byte[]> get()
        {
            if (!returnTimestampedStore)
            {
                return new RocksDBWindowStore(
                    new RocksDBSegmentedBytesStore(
                        name,
                        metricsScope(),
                        retentionPeriod,
                        segmentInterval,
                        new WindowKeySchema()),
                    retainDuplicates,
                    windowSize);
            }
            else
            {
                return new RocksDBTimestampedWindowStore(
                    new RocksDBTimestampedSegmentedBytesStore(
                        name,
                        metricsScope(),
                        retentionPeriod,
                        segmentInterval,
                        new WindowKeySchema()),
                    retainDuplicates,
                    windowSize);
            }
        }

        public override string metricsScope()
        {
            return "rocksdb-window-state";
        }

        [System.Obsolete]
        public override int segments()
        {
            return (int)(retentionPeriod / segmentInterval) + 1;
        }

        public override long segmentIntervalMs()
        {
            return segmentInterval;
        }

        public override long windowSize()
        {
            return windowSize;
        }

        public override bool retainDuplicates()
        {
            return retainDuplicates;
        }

        public override long retentionPeriod()
        {
            return retentionPeriod;
        }
    }
}