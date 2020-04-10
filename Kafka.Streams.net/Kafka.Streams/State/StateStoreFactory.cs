using System.Collections.Generic;

namespace Kafka.Streams.State
{
    public interface IStateStoreFactory
    {
    }

    public interface IStateStoreFactory<out T> : IStateStoreFactory
        where T :IStateStore
    {
        IStoreBuilder<T> Builder { get; }
        IStateStore Build();
        HashSet<string> Users { get; }
        string Name { get; }
    }

    public class StateStoreFactory<T> : IStateStoreFactory<T>
        where T : IStateStore
    {
        public HashSet<string> Users { get; } = new HashSet<string>();
        
        public IStoreBuilder<T> Builder { get; }

        public StateStoreFactory(IStoreBuilder<T> builder)
        {
            this.Builder = builder;
        }

        public IStateStore Build()
        {
            return this.Builder.Build();
        }

        public string Name => this.Builder.Name;
    }
}
