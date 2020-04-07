using Kafka.Streams.Interfaces;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.RocksDbState;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RocksDbSharp;
using System;

namespace Kafka.Streams
{
    public static class StreamsBuilderExtensions
    {
        public static StreamsBuilder WithRocksDBPersistentStore(this StreamsBuilder builder, Action<DbOptions>? options = null)
        {
            var dbOtions = new DbOptions();
            options?.Invoke(dbOtions);

            builder.Context.ServiceCollection.TryAddScoped<ITimestampedKeyValueBytesStoreSupplier, RocksDbKeyTimestampedValueBytesStoreSupplier>();
            builder.Context.ServiceCollection.TryAddScoped<IKeyValueBytesStoreSupplier, RocksDbKeyValueBytesStoreSupplier>();

            return builder;
        }
    }
}
