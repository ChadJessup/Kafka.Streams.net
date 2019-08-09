using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;

namespace Kafka.Streams.KStream
{
    /**
     * The {@code Consumed} is used to define the optional parameters when using {@link StreamsBuilder} to
     * build instances of {@link KStream}, {@link KTable}, and {@link GlobalKTable}.
     * <p>
     * For example, you can read a topic as {@link KStream} with a custom timestamp extractor and specify the corresponding
     * key and value serdes like:
     * <pre>{@code
     * StreamsBuilder builder = new StreamsBuilder();
     * KStream<string, long> stream = builder.stream(
     *   "topicName",
     *   Consumed.with(Serdes.string(), Serdes.Long())
     *           .withTimestampExtractor(new LogAndSkipOnInvalidTimestamp()));
     * }</pre>
     * Similarly, you can read a topic as {@link KTable} with a custom {@code auto.offset.reset} configuration and force a
     * state store {@link org.apache.kafka.streams.kstream.Materialized materialization} to access the content via
     * interactive queries:
     * <pre>{@code
     * StreamsBuilder builder = new StreamsBuilder();
     * KTable<int, int> table = builder.table(
     *   "topicName",
     *   Consumed.with(AutoOffsetReset.LATEST),
     *   Materialized.As("queryable-store-name"));
     * }</pre>
     *
     * @param type of record key
     * @param type of record value
     */
    public class Consumed<K, V> : INamedOperation<Consumed<K, V>>
    {
        public ISerde<K> keySerde { get; private set; }
        public ISerde<V> valueSerde { get; private set; }
        public ITimestampExtractor timestampExtractor;
        protected AutoOffsetReset resetPolicy;
        protected string processorName;

        private Consumed(
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            ITimestampExtractor timestampExtractor,
            AutoOffsetReset resetPolicy,
            string processorName)
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
            : this(consumed.keySerde,
                 consumed.valueSerde,
                 consumed.timestampExtractor,
                 consumed.resetPolicy,
                 consumed.processorName)
        {
        }

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
        public static Consumed<K, V> with(
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            ITimestampExtractor timestampExtractor,
            AutoOffsetReset resetPolicy)
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
        public static Consumed<K, V> with(
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            return new Consumed<K, V>(keySerde, valueSerde, null, AutoOffsetReset.UNKNOWN, null);
        }

        /**
         * Create an instance of {@link Consumed} with a {@link TimestampExtractor}.
         *
         * @param timestampExtractor the timestamp extractor to used. If {@code null} the default timestamp extractor from config will be used
         * @param                key type
         * @param                value type
         * @return a new instance of {@link Consumed}
         */
        public static Consumed<K, V> with(ITimestampExtractor timestampExtractor)
        {
            return new Consumed<K, V>(null, null, timestampExtractor, AutoOffsetReset.UNKNOWN, null);
        }

        /**
         * Create an instance of {@link Consumed} with a {@link org.apache.kafka.streams.AutoOffsetReset AutoOffsetReset}.
         *
         * @param resetPolicy the offset reset policy to be used. If {@code null} the default reset policy from config will be used
         * @param         key type
         * @param         value type
         * @return a new instance of {@link Consumed}
         */
        public static Consumed<K, V> with(AutoOffsetReset resetPolicy)
        {
            return new Consumed<K, V>(null, null, null, resetPolicy, null);
        }

        /**
         * Create an instance of {@link Consumed} with provided processor name.
         *
         * @param processorName the processor name to be used. If {@code null} a default processor name will be generated
         * @param         key type
         * @param         value type
         * @return a new instance of {@link Consumed}
         */
        public static Consumed<K, V> As(string processorName)
        {
            return new Consumed<K, V>(null, null, null, AutoOffsetReset.UNKNOWN, processorName);
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
         * Configure the instance of {@link Consumed} with a {@link TimestampExtractor}.
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
         * Configure the instance of {@link Consumed} with a processor name.
         *
         * @param processorName the processor name to be used. If {@code null} a default processor name will be generated
         * @return this
         */
        public Consumed<K, V> withName(string processorName)
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

            return object.Equals(keySerde, consumed.keySerde)
                && object.Equals(valueSerde, consumed.valueSerde)
                && object.Equals(timestampExtractor, consumed.timestampExtractor)
                && resetPolicy == consumed.resetPolicy;
        }

        public override int GetHashCode()
        {
            return (keySerde, valueSerde, timestampExtractor, resetPolicy).GetHashCode();
        }
    }
}
