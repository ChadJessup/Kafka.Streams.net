using Kafka.Streams.Processors;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableSource<K, V> : IProcessorSupplier<K, V>
    {
        private readonly ILogger logger;
        private readonly IServiceProvider services;

        private readonly string? storeName;
        public string? queryableName { get; private set; }
        private bool sendOldValues;

        public KTableSource(
            ILogger<KTableSource<K, V>> logger,
            IServiceProvider serviceProvider,
            string? storeName,
            string? queryableName)
        {
            this.storeName = storeName;
            
            this.logger = logger;
            this.services = serviceProvider;
            
            this.storeName = storeName;
            this.queryableName = queryableName;
            this.sendOldValues = false;
        }

        public IKeyValueProcessor<K, V> Get()
        {
            return ActivatorUtilities.CreateInstance<KTableSourceProcessor<K, V>>(
                this.services,
                this.queryableName,
                this.sendOldValues);
        }

        // when source ktable requires sending old values, we just
        // need to set the queryable name as the store name to enforce materialization
        public void EnableSendingOldValues()
        {
            this.sendOldValues = true;
            this.queryableName = storeName;
        }

        // when the source ktable requires materialization from downstream, we just
        // need to set the queryable name as the store name to enforce materialization
        public void Materialize()
        {
            this.queryableName = storeName;
        }
    }
}