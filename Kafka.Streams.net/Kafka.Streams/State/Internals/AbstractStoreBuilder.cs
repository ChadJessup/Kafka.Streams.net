using Kafka.Streams.Interfaces;

using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public abstract class AbstractStoreBuilder<K, V, T> : IStoreBuilder<T>
        where T : IStateStore
    {
        public Dictionary<string, string> logConfig { get; private set; } = new Dictionary<string, string>();
        public string name { get; }
        public bool loggingEnabled { get; }

        public ISerde<K> keySerde { get; }
        public ISerde<V> valueSerde { get; }
        public KafkaStreamsContext context { get; }
        public bool enableCaching { get; private set; }
        public bool enableLogging { get; private set; } = true;

        public AbstractStoreBuilder(
            KafkaStreamsContext context,
            string name,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            this.context = context ?? throw new ArgumentNullException(nameof(context));
            this.name = name ?? throw new ArgumentNullException(nameof(name));
            this.valueSerde = valueSerde;
            this.keySerde = keySerde;
        }

        public IStoreBuilder<T> WithCachingEnabled()
        {
            enableCaching = true;

            return this;
        }

        public IStoreBuilder<T> WithCachingDisabled()
        {
            enableCaching = false;

            return this;
        }

        public IStoreBuilder<T> WithLoggingEnabled(Dictionary<string, string> config)
        {
            config = config ?? throw new ArgumentNullException(nameof(config));

            enableLogging = true;
            logConfig = config;


            return this;
        }

        public IStoreBuilder<T> WithLoggingDisabled()
        {
            enableLogging = false;
            logConfig.Clear();

            return this;
        }

        public abstract T Build();
    }
}
