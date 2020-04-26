using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.KStream.Interfaces
{
    /**
     * The {@code ValueTransformer} interface for stateful mapping of a value to a new value (with possible new type).
     * This is a stateful record-by-record operation, i.e, {@link #transform(object)} is invoked individually for each
     * record of a stream and can access and modify a state that is available beyond a single call of
     * {@link #transform(object)} (cf. {@link ValueMapper} for stateless value transformation).
     * Additionally, this {@code ValueTransformer} can {@link IProcessorContext#schedule(TimeSpan, PunctuationType, Punctuator) schedule}
     * a method to be {@link Punctuator#punctuate(long) called periodically} with the provided context.
     * If {@code ValueTransformer} is applied to a {@link KeyValuePair} pair record the record's key is preserved.
     * <p>
     * Use {@link ValueTransformerSupplier} to provide new instances of {@code ValueTransformer} to Kafka Stream's runtime.
     * <p>
     * If a record's key and value should be modified {@link Transformer} can be used.
     *
     * @param  value type
     * @param transformed value type
     * @see ValueTransformerSupplier
     * @see ValueTransformerWithKeySupplier
     * @see KStream#transformValues(ValueTransformerSupplier, string...)
     * @see KStream#transformValues(ValueTransformerWithKeySupplier, string...)
     * @see Transformer
     */
    public interface IValueTransformer<V, VR>
    {
        /**
         * Initialize this transformer.
         * This is called once per instance when the topology gets initialized.
         * When the framework is done with the transformer, {@link #Close()} will be called on it; the
         * framework may later re-use the transformer by calling {@link #Init(IProcessorContext)} again.
         * <p>
         * The provided {@link IProcessorContext context} can be used to access topology and record meta data, to
         * {@link IProcessorContext#schedule(TimeSpan, PunctuationType, Punctuator) schedule} a method to be
         * {@link Punctuator#punctuate(long) called periodically} and to access attached {@link IStateStore}s.
         * <p>
         * Note that {@link IProcessorContext} is updated in the background with the current record's meta data.
         * Thus, it only contains valid record meta data when accessed within {@link #transform(object)}.
         * <p>
         * Note that using {@link IProcessorContext#forward(object, object)} or
         * {@link IProcessorContext#forward(object, object, To)} is not allowed within any method of
         * {@code ValueTransformer} and will result in an {@link StreamsException exception}.
         *
         * @param context the context
         * @throws InvalidOperationException If store gets registered after initialization is already finished
         * @throws StreamsException if the store's change log does not contain the partition
         */
        public void Init<K>(IProcessorContext context)
        {
        }

        /**
         * Transform the given value to a new value.
         * Additionally, any {@link IStateStore} that is {@link KStream#transformValues(ValueTransformerSupplier, string...)
         * attached} to this operator can be accessed and modified arbitrarily (cf.
         * {@link IProcessorContext#getStateStore(string)}).
         * <p>
         * Note, that using {@link IProcessorContext#forward(object, object)} or
         * {@link IProcessorContext#forward(object, object, To)} is not allowed within {@code transform} and
         * will result in an {@link StreamsException exception}.
         *
         * @param value the value to be transformed
         * @return the new value
         */
        public VR Transform(V value)
        {
            return (VR)(object)value;
        }

        /**
         * Close this transformer and clean up any resources. The framework may
         * later re-use this transformer by calling {@link #Init(IProcessorContext)} on it again.
         * <p>
         * It is not possible to return any new output records within {@code Close()}.
         * Using {@link IProcessorContext#forward(object, object)} or {@link IProcessorContext#forward(object, object, To)}
         * will result in an {@link StreamsException exception}.
         */
        public void Close()
        {
        }
    }
}
