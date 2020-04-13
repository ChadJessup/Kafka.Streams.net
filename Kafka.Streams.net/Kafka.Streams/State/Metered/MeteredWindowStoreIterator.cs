using System;
using System.Collections;
using System.Collections.Generic;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.State.Metered
{
    public class MeteredWindowStoreIterator<V> : IWindowStoreIterator<V>
    {
        public KafkaStreamsContext Context { get; }
        public KeyValuePair<DateTime, V> Current { get; }
        object IEnumerator.Current { get; }

        private IWindowStoreIterator<byte[]> iter;
        private IStateSerdes<long, V> serdes;
        private long startNs;

        public MeteredWindowStoreIterator(
            KafkaStreamsContext context,
            IWindowStoreIterator<byte[]> iter,
            IStateSerdes<long, V> serdes)
        {
            this.Context = context;
            this.iter = iter;
            //this.sensor = sensor;
            //this.metrics = metrics;
            this.serdes = serdes;
            this.startNs = context.Clock.NowAsEpochNanoseconds;
        }

        public bool HasNext()
        {
            return true;//iter.HasNext();
        }

        public KeyValuePair<DateTime, V> Next()
        {
            KeyValuePair<DateTime, byte[]> next = this.iter.Current;
            return KeyValuePair.Create(next.Key, this.serdes.ValueFrom(next.Value));
        }

        public void Remove()
        {
            //this.iter.Remove();
        }

        public void Close()
        {
            try
            {
                this.iter.Close();
            }
            finally
            {
//                metrics.recordLatency(this.sensor, this.startNs, this.Context.Clock.NowAsEpochNanoseconds);
            }
        }

        public DateTime PeekNextKey()
        {
            return this.iter.PeekNextKey();
        }

        public bool MoveNext()
        {
            return true;
        }

        public void Reset()
        {
        }

        public void Dispose()
        {
        }
    }
}
