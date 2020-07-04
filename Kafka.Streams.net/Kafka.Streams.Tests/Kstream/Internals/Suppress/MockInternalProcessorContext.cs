using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.Tasks;
using System;
using System.Collections.Generic;
using System.IO;

namespace Kafka.Streams.Tests.Kstream.Internals.Suppress
{
    internal class MockInternalProcessorContext : IProcessorContext
    {
        public string ApplicationId { get; }
        public TaskId TaskId { get; }
        public ISerde KeySerde { get; }
        public ISerde ValueSerde { get; }
        public DirectoryInfo StateDir { get; }
        public string Topic { get; }
        public int Partition { get; }
        public long Offset { get; }
        public Headers Headers { get; }
        public DateTime Timestamp { get; }

        internal void SetRecordMetadata(string v1, int v2, long v3, object p, long timestamp)
        {
            throw new NotImplementedException();
        }

        internal void setCurrentNode(ProcessorNode processorNode)
        {
            throw new NotImplementedException();
        }

        internal IEnumerable<object> Forwarded()
        {
            throw new NotImplementedException();
        }

        public void Register(IStateStore store, IStateRestoreCallback stateRestoreCallback)
        {
            throw new NotImplementedException();
        }

        public IStateStore GetStateStore(string Name)
        {
            throw new NotImplementedException();
        }

        public ICancellable Schedule(TimeSpan interval, PunctuationType? type, IPunctuator? callback)
        {
            throw new NotImplementedException();
        }

        public ICancellable Schedule(TimeSpan interval, PunctuationType type, Action<DateTime> callback)
        {
            throw new NotImplementedException();
        }

        public void Forward<K1, V1>(K1 key, V1 value)
        {
            throw new NotImplementedException();
        }

        public void Forward<K1, V1>(K1 key, V1 value, To to)
        {
            throw new NotImplementedException();
        }

        public void Forward<K1, V1>(K1 key, V1 value, string childName)
        {
            throw new NotImplementedException();
        }

        public void Commit()
        {
            throw new NotImplementedException();
        }

        public Dictionary<string, object> AppConfigs()
        {
            throw new NotImplementedException();
        }

        public Dictionary<string, object> AppConfigsWithPrefix(string prefix)
        {
            throw new NotImplementedException();
        }
    }
}
