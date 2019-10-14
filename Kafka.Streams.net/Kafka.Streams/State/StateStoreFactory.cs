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
        public IStoreBuilder<T> Builder { get; }

        public StateStoreFactory(IStoreBuilder<T> builder)
        {
            this.Builder = builder;
        }

        public IStateStore Build()
        {
            return Builder.Build();
        }

        private long RetentionPeriod<K, V>()
        {
            if (this.Builder is WindowStoreBuilder<K, V>)
            {
                return ((WindowStoreBuilder<K, V>)this.Builder).retentionPeriod();
            }
            else if (this.Builder is TimestampedWindowStoreBuilder<K, V>)
            {
                return ((TimestampedWindowStoreBuilder<K, V>)this.Builder).retentionPeriod();
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

        public string name => this.Builder.name;

        private bool IsWindowStore<K, V>()
        {
            return this.Builder is WindowStoreBuilder<K, V>
            || this.Builder is TimestampedWindowStoreBuilder<K, V>;
            // || builder is SessionStoreBuilder<K, V>;
        }
    }
}
