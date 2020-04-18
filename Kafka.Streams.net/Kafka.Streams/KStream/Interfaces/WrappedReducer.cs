using System;

namespace Kafka.Streams.KStream.Interfaces
{
    public class WrappedReducer<V> : IReducer<V>
    {
        private Func<V, V, V> reducer;

        public WrappedReducer(Func<V, V, V> reducer)
        {
            this.reducer = reducer;
        }

        public V Apply(V value1, V value2)
            => this.reducer(value1, value2);
    }
}
