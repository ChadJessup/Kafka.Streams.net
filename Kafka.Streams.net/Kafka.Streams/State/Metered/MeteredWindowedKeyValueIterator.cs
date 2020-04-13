using System;
using System.Collections;
using System.Collections.Generic;
using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.Metered
{
    public class MeteredWindowedKeyValueIterator<K, V> : IKeyValueIterator<IWindowed<K>, V>
    {
        private IKeyValueIterator<IWindowed<Bytes>, byte[]> iter;
        private IStateSerdes<K, V> serdes;
        private long startNs;
        private KafkaStreamsContext context;

        public MeteredWindowedKeyValueIterator(
            KafkaStreamsContext context,
            IKeyValueIterator<IWindowed<Bytes>, byte[]> iter,
            IStateSerdes<K, V> serdes)
        {
            this.iter = iter ?? throw new ArgumentNullException(nameof(iter));
            this.serdes = serdes ?? throw new ArgumentNullException(nameof(serdes));
            this.context = context ?? throw new ArgumentNullException(nameof(context));

            this.startNs = context.Clock.NowAsEpochNanoseconds;
        }

        private IWindowed<K> WindowedKey(IWindowed<Bytes> bytesKey)
        {
            K key = this.serdes.KeyFrom(bytesKey.Key.Get());

            return new Windowed<K>(key, bytesKey.Window);
        }

        public KeyValuePair<IWindowed<K>, V> Current
        {
            get
            {
                KeyValuePair<IWindowed<Bytes>, byte[]> next = this.iter.Current;
                return KeyValuePair.Create(this.WindowedKey(next.Key), this.serdes.ValueFrom(next.Value));
            }
        }

        object IEnumerator.Current => this.iter.Current;

        public void Close()
        {
            try
            {
                this.iter.Close();
            }
            finally
            {
                //metrics.recordLatency(sensor, startNs, this.Context.Clock.NowAsEpochNanoseconds;);
            }
        }

        public IWindowed<K> PeekNextKey() => this.WindowedKey(this.iter.PeekNextKey());
        public bool MoveNext() => this.iter.MoveNext();
        public void Reset() => this.iter.Reset();
        public void Dispose() => this.iter.Dispose();
    }
}
