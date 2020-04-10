
namespace Kafka.Streams.KStream.Internals.Suppress
{
    /**
     * Internally-facing interface to work around the fact that All Suppressed config objects
     * are Name-able, but do not present a getter (for consistency with other config objects).
     * If we allow getters on config objects in the future, we can delete this interface.
     */
    public interface INamedSuppressed<K> : ISuppressed<K>
    {
        string Name { get; }
    }
}
