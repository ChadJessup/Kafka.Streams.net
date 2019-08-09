using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableSessionWindowValueGetter : IKTableValueGetter<Windowed<K>, Agg>
{
    private ISessionStore<K, Agg> store;



    public void init(IProcessorContext<K, V> context)
    {
        store = (ISessionStore<K, Agg>)context.getStateStore(storeName);
    }


    public ValueAndTimestamp<Agg> get(Windowed<K> key)
    {
        return ValueAndTimestamp.make(
            store.fetchSession(key.key(), key.window().start(), key.window().end()),
            key.window().end());
    }


    public void close()
    {
    }
}
}