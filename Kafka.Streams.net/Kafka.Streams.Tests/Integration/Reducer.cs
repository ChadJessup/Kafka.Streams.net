using Kafka.Streams.KStream;
using System;

namespace Kafka.Streams.Tests.Integration
{
    internal class Reducer2<T> : IReducer2<T>
    {
        private readonly Func<T, T, T> reducer;

        public Reducer2(Func<T, T, T> reducer)
            => this.reducer = reducer ?? throw new ArgumentNullException(nameof(reducer));

        public T Apply(T value1, T value2)
            => this.reducer(value1, value2);
    }
}
