using Kafka.Streams.KStream;
using System;

namespace Kafka.Streams.Tests.Integration
{
    internal class Reducer<T> : IReducer<T>
    {
        private readonly Func<T, T, T> reducer;

        public Reducer(Func<T, T, T> reducer)
            => this.reducer = reducer ?? throw new ArgumentNullException(nameof(reducer));

        public T Apply(T value1, T value2)
            => this.reducer(value1, value2);
    }
}
