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
package org.apache.kafka.streams.state.internals;

using Kafka.Common.Utils.Bytes;
using Kafka.Streams.State.SessionBytesStoreSupplier;
using Kafka.Streams.State.SessionStore;

public class InMemorySessionBytesStoreSupplier : SessionBytesStoreSupplier
{
    private string name;
    private long retentionPeriod;

    public InMemorySessionBytesStoreSupplier(string name,
                                             long retentionPeriod)
{
        this.name = name;
        this.retentionPeriod = retentionPeriod;
    }

    public override string name()
{
        return name;
    }

    public override SessionStore<Bytes, byte[]> get()
{
        return new InMemorySessionStore(name, retentionPeriod, metricsScope());
    }

    public override string metricsScope()
{
        return "in-memory-session-state";
    }

    // In-memory store is not *really* segmented, so just say it is 1 (for ordering consistency with caching enabled)
    public override long segmentIntervalMs()
{
        return 1;
    }

    public override long retentionPeriod()
{
        return retentionPeriod;
    }
}

