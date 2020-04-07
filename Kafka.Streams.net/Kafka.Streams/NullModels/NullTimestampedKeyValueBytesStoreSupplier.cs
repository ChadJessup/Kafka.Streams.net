using System;
using System.Collections.Generic;
using System.Text;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.NullModels
{
    public class NullTimestampedKeyValueBytesStoreSupplier : ITimestampedKeyValueBytesStoreSupplier
    {
        public string Name { get; private set; } = nameof(NullTimestampedKeyValueBytesStoreSupplier);

        public IKeyValueStore<Bytes, byte[]> Get()
        {
            return new NullKeyValueStore();
        }

        public void SetName(string name) => this.Name = name;
    }
}
