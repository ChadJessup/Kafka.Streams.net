using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.State;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class MaterializedInternal<K, V, S> : Materialized<K, V, S>
        where S : IStateStore
    {
        private readonly bool queriable;

        public MaterializedInternal(Materialized<K, V, S> materialized)
            : this(materialized, null, null)
        {
        }

        public MaterializedInternal(
            Materialized<K, V, S> materialized,
            IInternalNameProvider nameProvider,
            string generatedStorePrefix)
            : base(materialized)
        {
            // if storeName is not provided, the corresponding KTable would never be queryable;
            // but we still need to provide an internal Name for it in case we materialize.
            this.queriable = this.StoreName != null;

            if (!this.queriable && nameProvider != null)
            {
                this.StoreName = nameProvider.NewStoreName(generatedStorePrefix);
            }
        }

        public string? QueryableStoreName()
        {
            return this.queriable
                ? this.StoreName
                : null;
        }

        private string? _storeName;
        public override string? StoreName
        {
            get => this.StoreSupplier?.Name ?? this._storeName;
            protected set => this._storeName = value;
        }

        public Dictionary<string, string> LogConfig()
        {
            return this.TopicConfig;
        }

        public static MaterializedInternal<K, V, S> ToMaterializedInternal(MaterializedInternal<K, V, S> left, MaterializedInternal<K, V, S> right)
        {
            throw new NotImplementedException();
        }
    }
}