using Kafka.Streams.State.KeyValues;
using Kafka.Streams;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Mocks
{
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
        string IStateStore.Name { get; }

        public void Add(int key, byte[] value)
        {
            throw new System.NotImplementedException();
        }

        public IKeyValueIterator<int, byte[]> All()
        {
            throw new System.NotImplementedException();
        }

        public void Close()
        {
            throw new System.NotImplementedException();
        }

        public byte[] Delete(int key)
        {
            throw new System.NotImplementedException();
        }

        public void Flush()
        {
            throw new System.NotImplementedException();
        }

        public byte[] Get(int key)
        {
            throw new System.NotImplementedException();
        }

        public void Init(IProcessorContext context, IStateStore root)
        {
            throw new System.NotImplementedException();
        }

        public bool IsOpen()
        {
            throw new System.NotImplementedException();
        }

        public bool IsPresent()
        {
            throw new System.NotImplementedException();
        }

        public void PutAll(List<KeyValuePair<int, byte[]>> entries)
        {
            throw new System.NotImplementedException();
        }

        public byte[] PutIfAbsent(int key, byte[] value)
        {
            throw new System.NotImplementedException();
        }

        public IKeyValueIterator<int, byte[]> Range(int from, int to)
        {
            throw new System.NotImplementedException();
        }

        bool IStateStore.Persistent()
        {
            throw new System.NotImplementedException();
        }
    }
}