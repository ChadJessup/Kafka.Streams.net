using Kafka.Streams.KStream;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.Tests.Mocks
{
    public class MockKeyValueStoreBuilder : AbstractStoreBuilder<int, byte[], IKeyValueStore<int, byte[]>>
    {
        private readonly bool Persistent;

        public MockKeyValueStoreBuilder(KafkaStreamsContext context, string storeName, bool Persistent)
            : base(context, storeName, Serdes.Int(), Serdes.ByteArray())
        {
            this.Persistent = Persistent;
        }

        public override IKeyValueStore<int, byte[]> Build()
            => new MockKeyValueStore(this.Name, this.Persistent);
    }
}