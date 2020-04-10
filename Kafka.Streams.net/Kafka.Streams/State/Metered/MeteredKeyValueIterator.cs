using System.Collections;
using System.Collections.Generic;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.Metered
{
    public class MeteredKeyValueIterator<K, V> : IKeyValueIterator<K, V>
    {

        private IKeyValueIterator<Bytes, byte[]> iter;
        private long startNs;

        public KeyValuePair<K, V> Current
        {
            get;
        }
        object IEnumerator.Current
        {
            get;
        }

        public MeteredKeyValueIterator(
            KafkaStreamsContext context,
            IKeyValueIterator<Bytes, byte[]> iter)
        {
            this.iter = iter;
            //this.sensor = sensor;
            this.startNs = context.Clock.NowAsEpochNanoseconds;
        }

        public bool HasNext()
        {
            return false;// iter.HasNext();
        }


        public KeyValuePair<K, V> next()
        {
            KeyValuePair<Bytes, byte[]> keyValue = iter.Current;
            return new KeyValuePair<K, V>();
            //KeyValuePair.Create(
            //serdes.keyFrom(keyValue.Key),
            //outerValue(keyValue.Value));
        }


        public void Remove()
        {
            // iter.Remove();
        }


        public void Close()
        {
            try
            {
                iter.Close();
            }
            finally
            {
                // metrics.recordLatency(sensor, startNs, this.Context.Clock.NowAsEpochNanoseconds;);
            }
        }

        public K PeekNextKey()
        {
            return default; //serdes.keyFrom(iter.PeekNextKey()());
        }

        public bool MoveNext()
        {
            throw new System.NotImplementedException();
        }

        public void Reset()
        {
        }

        public void Dispose()
        {
        }
    }
}
