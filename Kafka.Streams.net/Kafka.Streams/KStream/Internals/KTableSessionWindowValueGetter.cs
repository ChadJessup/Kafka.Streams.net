//using Kafka.Streams.State;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableSessionWindowValueGetter : IKTableValueGetter<Windowed<K>, Agg>
//{
//    private ISessionStore<K, Agg> store;



//    public void init(IProcessorContext context)
//    {
//        store = (ISessionStore<K, Agg>)context.getStateStore(storeName);
//    }


//    public ValueAndTimestamp<Agg> get(Windowed<K> key)
//    {
//        return ValueAndTimestamp.Make(
//            store.FetchSession(key.key(), key.window().start(), key.window().end()),
//            key.window().end());
//    }


//    public void close()
//    {
//    }
//}
//}