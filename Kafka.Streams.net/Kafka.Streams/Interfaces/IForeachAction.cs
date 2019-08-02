namespace Kafka.Streams.Interfaces
{
    /**
 * The {@code ForeachAction} interface for performing an action on a {@link org.apache.kafka.streams.KeyValue key-value
 * pair}.
 * This is a stateless record-by-record operation, i.e, {@link #apply(Object, Object)} is invoked individually for each
 * record of a stream.
 * If stateful processing is required, consider using
 * {@link KStream#process(org.apache.kafka.streams.processor.ProcessorSupplier, String...) KStream#process(...)}.
 *
 * @param <K> key type
 * @param <V> value type
 * @see KStream#foreach(ForeachAction)
 */
    public interface IForeachAction<K, V>
    {

        /**
         * Perform an action for each record of a stream.
         *
         * @param key   the key of the record
         * @param value the value of the record
         */
        void Apply(K key, V value);
    }
}