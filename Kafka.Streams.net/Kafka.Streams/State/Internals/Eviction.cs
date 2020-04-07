using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors.Internals;
using System;

namespace Kafka.Streams.State.Internals
{
    public class Eviction<K, V>
    {
        public K key { get; }
        public IChange<V> value { get; }
        public ProcessorRecordContext recordContext { get; }

        public Eviction(K key, IChange<V> value, ProcessorRecordContext recordContext)
        {
            this.key = key;
            this.value = value;
            this.recordContext = recordContext;
        }

        public override string ToString()
        {
            return "Eviction{key=" + key + ", value=" + value + ", recordContext=" + recordContext + '}';
        }


        public override bool Equals(object o)
        {
            if (this == o) return true;
            if (o == null || GetType() != o.GetType()) return false;

            var eviction = (Eviction<object, object>)o;

            return key.Equals(eviction.key) &&
                value.Equals(eviction.value) &&
                recordContext.Equals(eviction.recordContext);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(this.key, this.value, this.recordContext);
        }
    }
}