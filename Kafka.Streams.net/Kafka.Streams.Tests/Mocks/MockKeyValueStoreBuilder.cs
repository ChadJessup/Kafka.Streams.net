using Kafka.Streams.KStream;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValue;

public class MockKeyValueStoreBuilder : AbstractStoreBuilder<int, byte[], IKeyValueStore<int, byte[]>>
{
    private readonly bool persistent;

    public MockKeyValueStoreBuilder(string storeName, bool persistent)
        : base(storeName, Serdes.Int(), Serdes.ByteArray(), null)//new MockTime())
    {

        this.persistent = persistent;
    }

    public override IKeyValueStore<int, byte[]> Build()
    {
        return new MockKeyValueStore(name, persistent);
    }
}
