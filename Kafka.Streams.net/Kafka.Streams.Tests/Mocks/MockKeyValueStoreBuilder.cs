using Kafka.Streams.KStream;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.Tests.Mocks
{
    public class MockKeyValueStoreBuilder : AbstractStoreBuilder<int, byte[], IKeyValueStore<int, byte[]>>
    {
        private readonly bool persistent;

        public MockKeyValueStoreBuilder(KafkaStreamsContext context, string storeName, bool persistent)
            : base(context, storeName, Serdes.Int(), Serdes.ByteArray())
        {
            this.persistent = persistent;
        }

        public override IKeyValueStore<int, byte[]> Build()
            => new MockKeyValueStore(name, persistent);
    }
}