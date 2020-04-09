using System;
using System.Collections;
using System.Collections.Generic;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.Metered
{
    public class MeteredWindowedKeyValueIterator<K, V> : IKeyValueIterator<Windowed<K>, V>
    {
        private IKeyValueIterator<Windowed<Bytes>, byte[]> iter;
        private IStateSerdes<K, V> serdes;
        private long startNs;
        private KafkaStreamsContext context;

        public MeteredWindowedKeyValueIterator(
            KafkaStreamsContext context,
            IKeyValueIterator<Windowed<Bytes>, byte[]> iter,
            IStateSerdes<K, V> serdes)
        {
            this.iter = iter ?? throw new ArgumentNullException(nameof(iter));
            this.serdes = serdes ?? throw new ArgumentNullException(nameof(serdes));
            this.context = context ?? throw new ArgumentNullException(nameof(context));

            this.startNs = context.Clock.NowAsEpochNanoseconds;
        }

        private Windowed<K> WindowedKey(Windowed<Bytes> bytesKey)
        {
            K key = serdes.KeyFrom(bytesKey.Key.Get());

            return new Windowed<K>(key, bytesKey.window);
        }

        public KeyValuePair<Windowed<K>, V> Current
        {
            get
            {
                KeyValuePair<Windowed<Bytes>, byte[]> next = iter.Current;
                return KeyValuePair.Create(WindowedKey(next.Key), serdes.ValueFrom(next.Value));
            }
        }

        object IEnumerator.Current => this.iter.Current;

        public void Close()
        {
            try
            {
                iter.Close();
            }
            finally
            {
                //metrics.recordLatency(sensor, startNs, time.nanoseconds());
            }
        }

        public Windowed<K> PeekNextKey() => WindowedKey(this.iter.PeekNextKey());
        public bool MoveNext() => this.iter.MoveNext();
        public void Reset() => this.iter.Reset();
        public void Dispose() => this.iter.Dispose();
    }
}
