using Kafka.Streams.Interfaces;

using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public abstract class AbstractStoreBuilder<K, V, T> : IStoreBuilder<T>
        where T : IStateStore
    {
        public Dictionary<string, string> logConfig { get; private set; } = new Dictionary<string, string>();
        public string Name { get; }
        public bool loggingEnabled { get; }

        public ISerde<K> keySerde { get; }
        public ISerde<V> valueSerde { get; }
        public KafkaStreamsContext context { get; }
        public bool enableCaching { get; private set; }
        public bool enableLogging { get; private set; } = true;

        public AbstractStoreBuilder(
            KafkaStreamsContext context,
            string Name,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            this.context = context ?? throw new ArgumentNullException(nameof(context));
            this.Name = Name ?? throw new ArgumentNullException(nameof(Name));
            this.valueSerde = valueSerde;
            this.keySerde = keySerde;
        }

        public IStoreBuilder<T> WithCachingEnabled()
        {
            this.enableCaching = true;

            return this;
        }

        public IStoreBuilder<T> WithCachingDisabled()
        {
            this.enableCaching = false;

            return this;
        }

        public IStoreBuilder<T> WithLoggingEnabled(Dictionary<string, string> config)
        {
            config = config ?? throw new ArgumentNullException(nameof(config));

            this.enableLogging = true;
            this.logConfig = config;


            return this;
        }

        public IStoreBuilder<T> WithLoggingDisabled()
        {
            this.enableLogging = false;
            this.logConfig.Clear();

            return this;
        }

        public abstract T Build();
    }
}
