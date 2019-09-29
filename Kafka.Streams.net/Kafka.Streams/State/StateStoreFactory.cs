using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State
{
    public class StateStoreFactory
    {
        protected HashSet<string> users { get; } = new HashSet<string>();
    }

    public class StateStoreFactory<T> : StateStoreFactory
        where T : IStateStore
    {
        private readonly IStoreBuilder<T> builder;

        public StateStoreFactory(IStoreBuilder<T> builder)
        {
            this.builder = builder;
        }

        public IStateStore build()
        {
            return builder.build();
        }

        long retentionPeriod<K, V>()
        {
            if (builder is WindowStoreBuilder<K, V>)
            {
                return ((WindowStoreBuilder<K, V>)builder).retentionPeriod();
            }
            else if (builder is TimestampedWindowStoreBuilder<K, V>)
            {
                return ((TimestampedWindowStoreBuilder<K, V>)builder).retentionPeriod();
            }
            //else if (builder is SessionStoreBuilder<K, V>)
            //{
            //    return ((SessionStoreBuilder<K, V>)builder).retentionPeriod();
            //}
            else
            {
                throw new InvalidOperationException("retentionPeriod is not supported when not a window store");
            }
        }

        public string name => builder.name;

        private bool isWindowStore<K, V>()
        {
            return builder is WindowStoreBuilder<K, V>
            || builder is TimestampedWindowStoreBuilder<K, V>;
//            || builder is SessionStoreBuilder<K, V>;
        }
    }
}
