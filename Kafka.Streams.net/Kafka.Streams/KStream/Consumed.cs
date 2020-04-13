using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;

namespace Kafka.Streams.KStream
{
    public static class Consumed
    {

        /**
         * Create an instance of {@link Consumed} with the supplied arguments. {@code null} values are acceptable.
         *
         * @param keySerde           the key serde. If {@code null} the default key serde from config will be used
         * @param valueSerde         the value serde. If {@code null} the default value serde from config will be used
         * @param timestampExtractor the timestamp extractor to used. If {@code null} the default timestamp extractor from config will be used
         * @param resetPolicy        the offset reset policy to be used. If {@code null} the default reset policy from config will be used
         * @param                key type
         * @param                value type
         * @return a new instance of {@link Consumed}
         */
        public static Consumed<K, V> With<K, V>(
            ISerde<K>? keySerde,
            ISerde<V>? valueSerde,
            ITimestampExtractor? timestampExtractor,
            AutoOffsetReset? resetPolicy)
        {
            return new Consumed<K, V>(keySerde, valueSerde, timestampExtractor, resetPolicy, null);
        }

        /**
         * Create an instance of {@link Consumed} with key and value {@link Serde}s.
         *
         * @param keySerde   the key serde. If {@code null} the default key serde from config will be used
         * @param valueSerde the value serde. If {@code null} the default value serde from config will be used
         * @param        key type
         * @param        value type
         * @return a new instance of {@link Consumed}
         */
        public static Consumed<K, V> With<K, V>(
            ISerde<K>? keySerde,
            ISerde<V>? valueSerde)
        {
            return new Consumed<K, V>(keySerde, valueSerde, null, null, null);
        }

        /**
         * Create an instance of {@link Consumed} with a {@link ITimestampExtractor}.
         *
         * @param timestampExtractor the timestamp extractor to used. If {@code null} the default timestamp extractor from config will be used
         * @param                key type
         * @param                value type
         * @return a new instance of {@link Consumed}
         */
        public static Consumed<K, V> With<K, V>(ITimestampExtractor timestampExtractor)
        {
            return new Consumed<K, V>(null, null, timestampExtractor, null, null);
        }

        /**
         * Create an instance of {@link Consumed} with a {@link org.apache.kafka.streams.AutoOffsetReset AutoOffsetReset}.
         *
         * @param resetPolicy the offset reset policy to be used. If {@code null} the default reset policy from config will be used
         * @param         key type
         * @param         value type
         * @return a new instance of {@link Consumed}
         */
        public static Consumed<K, V> With<K, V>(AutoOffsetReset resetPolicy)
        {
            return new Consumed<K, V>(null, null, null, resetPolicy, null);
        }

        /**
         * Create an instance of {@link Consumed} with provided processor Name.
         *
         * @param processorName the processor Name to be used. If {@code null} a default processor Name will be generated
         * @param         key type
         * @param         value type
         * @return a new instance of {@link Consumed}
         */
        public static Consumed<K, V> As<K, V>(string processorName)
        {
            return new Consumed<K, V>(null, null, null, null, processorName);
        }
    }

    /**
     * The {@code Consumed} is used to define the optional parameters when using {@link StreamsBuilder} to
     * build instances of {@link KStream}, {@link KTable}, and {@link GlobalKTable}.
     * <p>
     * For example, you can read a topic as {@link KStream} with a custom timestamp extractor and specify the corresponding
     * key and value serdes like:
     * <pre>{@code
     * StreamsBuilder builder = new StreamsBuilder();
     * IKStream<K, V> stream = builder.stream(
     *   "topicName",
     *   Consumed.With(Serdes.string(), Serdes.Long())
     *           .withTimestampExtractor(new LogAndSkipOnInvalidTimestamp()));
     * }</pre>
     * Similarly, you can read a topic as {@link KTable} with a custom {@code auto.offset.reset} configuration and force a
     * state store {@link org.apache.kafka.streams.kstream.Materialized materialization} to access the content via
     * interactive queries:
     * <pre>{@code
     * StreamsBuilder builder = new StreamsBuilder();
     * KTable<int, int> table = builder.table(
     *   "topicName",
     *   Consumed.With(AutoOffsetReset.LATEST),
     *   Materialized.As("queryable-store-Name"));
     * }</pre>
     *
     * @param type of record key
     * @param type of record value
     */
    public class Consumed<K, V> : INamedOperation<Consumed<K, V>>
    {
        public ISerde<K>? keySerde { get; private set; }
        public ISerde<V>? valueSerde { get; private set; }
        public ITimestampExtractor? timestampExtractor { get; private set; }
        protected AutoOffsetReset? resetPolicy { get; private set; }
        protected string? processorName { get; private set; }

        public Consumed(
            ISerde<K>? keySerde,
            ISerde<V>? valueSerde,
            ITimestampExtractor? timestampExtractor,
            AutoOffsetReset? resetPolicy,
            string? processorName)
        {
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.timestampExtractor = timestampExtractor;
            this.resetPolicy = resetPolicy;
            this.processorName = processorName;
        }

        /**
         * Create an instance of {@link Consumed} from an existing instance.
         * @param consumed  the instance of {@link Consumed} to copy
         */
        protected Consumed(Consumed<K, V> consumed)
            : this(consumed?.keySerde,
                 consumed?.valueSerde,
                 consumed?.timestampExtractor,
                 consumed?.resetPolicy,
                 consumed?.processorName)
        {
        }

        /**
         * Configure the instance of {@link Consumed} with a key {@link Serde}.
         *
         * @param keySerde the key serde. If {@code null}the default key serde from config will be used
         * @return this
         */
        public Consumed<K, V> WithKeySerde(ISerde<K> keySerde)
        {
            this.keySerde = keySerde;

            return this;
        }

        /**
         * Configure the instance of {@link Consumed} with a value {@link Serde}.
         *
         * @param valueSerde the value serde. If {@code null} the default value serde from config will be used
         * @return this
         */
        public Consumed<K, V> WithValueSerde(ISerde<V> valueSerde)
        {
            this.valueSerde = valueSerde;

            return this;
        }

        /**
         * Configure the instance of {@link Consumed} with a {@link ITimestampExtractor}.
         *
         * @param timestampExtractor the timestamp extractor to used. If {@code null} the default timestamp extractor from config will be used
         * @return this
         */
        public Consumed<K, V> WithTimestampExtractor(ITimestampExtractor timestampExtractor)
        {
            this.timestampExtractor = timestampExtractor;

            return this;
        }

        /**
         * Configure the instance of {@link Consumed} with a {@link org.apache.kafka.streams.AutoOffsetReset AutoOffsetReset}.
         *
         * @param resetPolicy the offset reset policy to be used. If {@code null} the default reset policy from config will be used
         * @return this
         */
        public Consumed<K, V> WithOffsetResetPolicy(AutoOffsetReset resetPolicy)
        {
            this.resetPolicy = resetPolicy;

            return this;
        }

        /**
         * Configure the instance of {@link Consumed} with a processor Name.
         *
         * @param processorName the processor Name to be used. If {@code null} a default processor Name will be generated
         * @return this
         */
        public Consumed<K, V> WithName(string processorName)
        {
            this.processorName = processorName;
            return this;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            var consumed = (Consumed<K, V>)o;

            return object.Equals(this.keySerde, consumed.keySerde)
                && object.Equals(this.valueSerde, consumed.valueSerde)
                && object.Equals(this.timestampExtractor, consumed.timestampExtractor)
                && this.resetPolicy == consumed.resetPolicy;
        }

        public override int GetHashCode()
        {
            return (this.keySerde, this.valueSerde, this.timestampExtractor, this.resetPolicy).GetHashCode();
        }
    }
}
