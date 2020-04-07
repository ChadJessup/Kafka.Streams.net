
namespace Kafka.Streams.Processors.Internals
{
    public class AtomicReference<T>
    {
        public AtomicReference(T reference) => this.Reference = reference;

        public T Reference { get; }
    }
}
