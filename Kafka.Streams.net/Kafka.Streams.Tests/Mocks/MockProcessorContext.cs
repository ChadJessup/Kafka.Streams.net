using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.Tasks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Kafka.Streams.Tests.Mocks
{
    public class MockProcessorContext : IProcessorContext
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

        public Dictionary<string, object> AppConfigs()
        {
            throw new NotImplementedException();
        }

        public Dictionary<string, object> AppConfigsWithPrefix(string prefix)
        {
            return new Dictionary<string, object>();
        }

        public void Commit()
        {
        }

        public void Forward<K1, V1>(K1 key, V1 value)
        {
        }

        public void Forward<K1, V1>(K1 key, V1 value, To to)
        {
        }

        public void Forward<K1, V1>(K1 key, V1 value, string childName)
        {
        }

        public IStateStore GetStateStore(string Name)
        {
            throw new NotImplementedException();
        }

        public void Register(IStateStore store, IStateRestoreCallback stateRestoreCallback)
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

        internal class CapturedForward
        {
        }
    }
}
