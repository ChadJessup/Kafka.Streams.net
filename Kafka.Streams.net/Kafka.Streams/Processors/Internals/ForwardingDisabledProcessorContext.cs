using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.Tasks;
using System;
using System.Collections.Generic;
using System.IO;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * {@code IProcessorContext} implementation that will throw on any forward call.
     */
    public class ForwardingDisabledProcessorContext<K, V> : IProcessorContext
    {
        private readonly IProcessorContext del;

        public ForwardingDisabledProcessorContext(IProcessorContext @delegate)
        {
            this.del = @delegate ?? throw new ArgumentNullException(nameof(@delegate));
        }

        public string ApplicationId
            => this.del.ApplicationId;

        public TaskId TaskId
            => this.del.TaskId;

        public ISerde KeySerde
            => this.del.KeySerde;

        public ISerde ValueSerde
            => this.del.ValueSerde;

        public DirectoryInfo StateDir
            => this.del.StateDir;

        //public IStreamsMetrics metrics
        //    => del.metrics;

        public void Register(IStateStore store, IStateRestoreCallback stateRestoreCallback)
        {
            this.del.Register(store, stateRestoreCallback);
        }

        public IStateStore GetStateStore(string Name)
        {
            return this.del.GetStateStore(Name);
        }

        [Obsolete]
        public ICancellable Schedule(
            long intervalMs,
            PunctuationType type,
            IPunctuator callback)
        {
            return this.del.Schedule(TimeSpan.FromMilliseconds(intervalMs), type, callback);
        }

        public ICancellable Schedule(
            TimeSpan interval,
            PunctuationType type,
            IPunctuator callback)
        {
            return this.del.Schedule(interval, type, callback);
        }
        public ICancellable Schedule(TimeSpan interval, PunctuationType type, Action<long> callback)
        {
            throw new NotImplementedException();
        }

        public void Forward<K1, V1>(K1 key, V1 value)
        {
            throw new StreamsException("IProcessorContext#forward() not supported.");
        }

        public void Forward<K1, V1>(K1 key, V1 value, To to)
        {
            throw new StreamsException("IProcessorContext#forward() not supported.");
        }

        [Obsolete]
        public void Forward<K1, V1>(K1 key, V1 value, int childIndex)
        {
            throw new StreamsException("IProcessorContext#forward() not supported.");
        }

        [Obsolete]
        public void Forward<K1, V1>(K1 key, V1 value, string childName)
        {
            throw new StreamsException("IProcessorContext#forward() not supported.");
        }

        public void Commit()
        {
            this.del.Commit();
        }

        public string Topic
            => this.del.Topic;

        public int Partition
            => this.del.Partition;

        public long Offset
            => this.del.Offset;

        public Headers Headers
            => this.del.Headers;

        public long Timestamp
            => this.del.Timestamp;

        public Dictionary<string, object> AppConfigs()
            => this.del.AppConfigs();

        public Dictionary<string, object> AppConfigsWithPrefix(string prefix)
            => this.del.AppConfigsWithPrefix(prefix);
    }
}
