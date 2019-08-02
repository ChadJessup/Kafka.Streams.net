namespace Kafka.Streams.Interfaces
{
    public interface INamedOperation<TNamedOperation>
        where TNamedOperation : INamedOperation<TNamedOperation>
    {
        /**
         * Sets the name to be used for an operation.
         *
         * @param name  the name to use.
         * @return an instance of {@link NamedOperation}
         */
        TNamedOperation WithName(string name);
    }
}