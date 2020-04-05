namespace Kafka.Streams.State.Internals
{
    public interface IClosingCallback
        {
            void DeregisterIterator(InMemorySessionStoreIterator iterator);
        }
}
