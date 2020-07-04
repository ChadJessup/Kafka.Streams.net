using System;

namespace Kafka.Streams.KStream.Interfaces
{
    public class WrappedReducer<V> : IReducer2<V>
    {
        private readonly Func<V, V, V> reducer;

        public WrappedReducer(Func<V, V, V> reducer)
        {
            this.reducer = reducer;
        }

        public WrappedReducer(Reducer<V> reducer1)
        {
        }

        public V Apply(V value1, V value2)
            => this.reducer(value1, value2);
    }
}
