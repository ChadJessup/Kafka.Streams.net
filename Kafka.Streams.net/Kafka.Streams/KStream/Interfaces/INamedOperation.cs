namespace Kafka.Streams.KStream.Interfaces
{
    /**
     * Default interface which can be used to personalized the named of operations, internal topics or store.
     */
    public interface INamedOperation<T>
        where T : INamedOperation<T>
    {

        /**
         * Sets the Name to be used for an operation.
         *
         * @param Name  the Name to use.
         * @return an instance of {@link NamedOperation}
         */
        T WithName(string Name);
    }
}