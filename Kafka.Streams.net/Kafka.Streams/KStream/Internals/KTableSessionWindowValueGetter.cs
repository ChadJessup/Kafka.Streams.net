//using Kafka.Streams.State;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableSessionWindowValueGetter : IKTableValueGetter<IWindowed<K>, Agg>
//{
//    private ISessionStore<K, Agg> store;



//    public void Init(IProcessorContext context)
//    {
//        store = (ISessionStore<K, Agg>)context.getStateStore(storeName);
//    }


//    public ValueAndTimestamp<Agg> get(IWindowed<K> key)
//    {
//        return ValueAndTimestamp.Make(
//            store.FetchSession(key.key(), key.window().start(), key.window().end()),
//            key.window().end());
//    }


//    public void Close()
//    {
//    }
//}
//}