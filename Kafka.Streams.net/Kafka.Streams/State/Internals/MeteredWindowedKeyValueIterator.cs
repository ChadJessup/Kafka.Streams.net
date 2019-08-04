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
namespace Kafka.Streams.State.Internals;

using Kafka.Common.metrics.Sensor;
using Kafka.Common.Utils.Bytes;
using Kafka.Common.Utils.Time;
using Kafka.Streams.KeyValue;
using Kafka.Streams.StreamsMetrics;
using Kafka.Streams.kstream.Windowed;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.StateSerdes;

class MeteredWindowedKeyValueIterator<K, V> : KeyValueIterator<Windowed<K>, V>
{

    private KeyValueIterator<Windowed<Bytes>, byte[]> iter;
    private Sensor sensor;
    private StreamsMetrics metrics;
    private StateSerdes<K, V> serdes;
    private long startNs;
    private ITime time;

    MeteredWindowedKeyValueIterator(KeyValueIterator<Windowed<Bytes>, byte[]> iter,
                                    Sensor sensor,
                                    StreamsMetrics metrics,
                                    StateSerdes<K, V> serdes,
                                    ITime time)
{
        this.iter = iter;
        this.sensor = sensor;
        this.metrics = metrics;
        this.serdes = serdes;
        this.startNs = time.nanoseconds();
        this.time = time;
    }

    public override bool hasNext()
{
        return iter.hasNext();
    }

    public override KeyValue<Windowed<K>, V> next()
{
        KeyValue<Windowed<Bytes>, byte[]> next = iter.next();
        return KeyValue.pair(windowedKey(next.key), serdes.valueFrom(next.value));
    }

    private Windowed<K> windowedKey(Windowed<Bytes> bytesKey)
{
        K key = serdes.keyFrom(bytesKey.key()());
        return new Windowed<>(key, bytesKey.window());
    }

    public override void Remove()
{
        iter.Remove();
    }

    public override void close()
{
        try
{
            iter.close();
        } finally
{
            metrics.recordLatency(sensor, startNs, time.nanoseconds());
        }
    }

    public override Windowed<K> peekNextKey()
{
        return windowedKey(iter.peekNextKey());
    }
}
