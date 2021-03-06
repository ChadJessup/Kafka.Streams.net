using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.KStream
{
    /**
     * The {@code ValueTransformerWithKey} interface for stateful mapping of a value to a new value (with possible new type).
     * This is a stateful record-by-record operation, i.e, {@link #transform(object, object)} is invoked individually for each
     * record of a stream and can access and modify a state that is available beyond a single call of
     * {@link #transform(object, object)} (cf. {@link ValueMapper} for stateless value transformation).
     * Additionally, this {@code ValueTransformerWithKey} can
     * {@link IProcessorContext#schedule(TimeSpan, PunctuationType, Punctuator) schedule} a method to be
     * {@link Punctuator#punctuate(long) called periodically} with the provided context.
     * Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
     * If {@code ValueTransformerWithKey} is applied to a {@link KeyValuePair} pair record the record's key is preserved.
     * <p>
     * Use {@link ValueTransformerWithKeySupplier} to provide new instances of {@link ValueTransformerWithKey} to
     * Kafka Stream's runtime.
     * <p>
     * If a record's key and value should be modified {@link Transformer} can be used.
     *
     * @param  key type
     * @param  value type
     * @param transformed value type
     * @see ValueTransformer
     * @see ValueTransformerWithKeySupplier
     * @see KStream#transformValues(ValueTransformerSupplier, string...)
     * @see KStream#transformValues(ValueTransformerWithKeySupplier, string...)
     * @see Transformer
     */

    public interface IValueTransformerWithKey<K, V, VR>
    {
        /**
         * Initialize this transformer.
         * This is called once per instance when the topology gets initialized.
         * <p>
         * The provided {@link IProcessorContext context} can be used to access topology and record meta data, to
         * {@link IProcessorContext#schedule(TimeSpan, PunctuationType, Punctuator) schedule} a method to be
         * {@link Punctuator#punctuate(long) called periodically} and to access attached {@link IStateStore}s.
         * <p>
         * Note that {@link IProcessorContext} is updated in the background with the current record's meta data.
         * Thus, it only contains valid record meta data when accessed within {@link #transform(object, object)}.
         * <p>
         * Note that using {@link IProcessorContext#forward(object, object)} or
         * {@link IProcessorContext#forward(object, object, To)} is not allowed within any method of
         * {@code ValueTransformerWithKey} and will result in an {@link StreamsException exception}.
         *
         * @param context the context
         * @throws InvalidOperationException If store gets registered after initialization is already finished
         * @throws StreamsException if the store's change log does not contain the partition
         */
        void Init(IProcessorContext context);

        /**
         * Transform the given [key and ]value to a new value.
         * Additionally, any {@link IStateStore} that is {@link KStream#transformValues(ValueTransformerWithKeySupplier, string...)
         * attached} to this operator can be accessed and modified arbitrarily (cf.
         * {@link IProcessorContext#getStateStore(string)}).
         * <p>
         * Note, that using {@link IProcessorContext#forward(object, object)} or
         * {@link IProcessorContext#forward(object, object, To)} is not allowed within {@code transform} and
         * will result in an {@link StreamsException exception}.
         *
         * @param readOnlyKey the read-only key
         * @param value       the value to be transformed
         * @return the new value
         */
        VR Transform(K readOnlyKey, V value);

        /**
         * Close this processor and clean up any resources.
         * <p>
         * It is not possible to return any new output records within {@code Close()}.
         * Using {@link IProcessorContext#forward(object, object)} or {@link IProcessorContext#forward(object, object, To)},
         * will result in an {@link StreamsException exception}.
         */
        void Close();
    }
}
