using Kafka.Streams.State.KeyValue;
using Kafka.Streams;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using System.Collections.Generic;

internal class MockKeyValueStore : IKeyValueStore<int, byte[]>
{
    private readonly string name;
    private readonly bool persistent;

    public MockKeyValueStore(string name, bool persistent)
    {
        this.name = name;
        this.persistent = persistent;
    }

    public long approximateNumEntries { get; }
    string IStateStore.name { get; }

    public void Add(int key, byte[] value)
    {
        throw new System.NotImplementedException();
    }

    public IKeyValueIterator<int, byte[]> all()
    {
        throw new System.NotImplementedException();
    }

    public void close()
    {
        throw new System.NotImplementedException();
    }

    public byte[] delete(int key)
    {
        throw new System.NotImplementedException();
    }

    public void flush()
    {
        throw new System.NotImplementedException();
    }

    public byte[] get(int key)
    {
        throw new System.NotImplementedException();
    }

    public void init(IProcessorContext context, IStateStore root)
    {
        throw new System.NotImplementedException();
    }

    public bool isOpen()
    {
        throw new System.NotImplementedException();
    }

    public bool isPresent()
    {
        throw new System.NotImplementedException();
    }

    public void putAll(List<KeyValue<int, byte[]>> entries)
    {
        throw new System.NotImplementedException();
    }

    public byte[] putIfAbsent(int key, byte[] value)
    {
        throw new System.NotImplementedException();
    }

    public IKeyValueIterator<int, byte[]> range(int from, int to)
    {
        throw new System.NotImplementedException();
    }

    bool IStateStore.persistent()
    {
        throw new System.NotImplementedException();
    }
}
