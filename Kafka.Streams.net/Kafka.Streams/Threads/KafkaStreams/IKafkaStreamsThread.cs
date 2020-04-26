using Confluent.Kafka;
using Kafka.Streams.Processors;
using Kafka.Streams.State;
using Kafka.Streams.State.Queryable;
using Kafka.Streams.Threads.Stream;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Threads.KafkaStreams
{
    public interface IKafkaStreamsThread : IThread<KafkaStreamsThreadStates>, IDisposable
    {
        IStreamThread[] Threads { get; }
        Dictionary<long, StreamThreadState> ThreadStates { get; }
        IStateListener GetStateListener();
        void Close();
        
        T Store<T>(string storeName, IQueryableStoreType<T> queryableStoreType);

        /**
         * Returns runtime information about the local threads of this {@link KafkaStreams} instance.
         *
         * @return the set of {@link ThreadMetadata}.
         */
        List<ThreadMetadata> LocalThreadsMetadata();

        StreamsMetadata MetadataForKey<K>(
            string storeName,
            K key,
            ISerializer<K> keySerializer);
    }
}
