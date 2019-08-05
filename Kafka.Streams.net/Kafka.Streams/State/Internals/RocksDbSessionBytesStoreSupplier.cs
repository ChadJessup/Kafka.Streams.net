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
    public class RocksDbSessionBytesStoreSupplier : SessionBytesStoreSupplier
    {
        private string name;
        private long retentionPeriod;

        public RocksDbSessionBytesStoreSupplier(string name,
                                                long retentionPeriod)
        {
            this.name = name;
            this.retentionPeriod = retentionPeriod;
        }

        public override string name()
        {
            return name;
        }

        public override ISessionStore<Bytes, byte[]> get()
        {
            RocksDBSegmentedBytesStore segmented = new RocksDBSegmentedBytesStore(
                name,
                metricsScope(),
                retentionPeriod,
                segmentIntervalMs(),
                new SessionKeySchema());
            return new RocksDBSessionStore(segmented);
        }

        public override string metricsScope()
        {
            return "rocksdb-session-state";
        }

        public override long segmentIntervalMs()
        {
            // Selected somewhat arbitrarily. Profiling may reveal a different value is preferable.
            return Math.Max(retentionPeriod / 2, 60_000L);
        }

        public override long retentionPeriod()
        {
            return retentionPeriod;
        }
    }
}