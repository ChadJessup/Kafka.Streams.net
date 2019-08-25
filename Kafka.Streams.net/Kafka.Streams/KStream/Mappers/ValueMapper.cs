﻿using Kafka.Streams.KStream.Interfaces;
using System;

namespace Kafka.Streams.KStream.Mappers
{
    public class ValueMapper<V, VR> : IValueMapper<V, VR>
    {
        private Func<V, VR> mapper;

        public ValueMapper(Func<V, VR> mapper)
            => this.mapper = mapper;

        public VR apply(V value)
            => this.mapper(value);
    }
}
