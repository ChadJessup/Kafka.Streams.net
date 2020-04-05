using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.KStream
{
    public static class Produced
    {
        /**
         * Create a Produced instance with provided keySerde, valueSerde, and partitioner.
         * @param keySerde      Serde to use for serializing the key
         * @param valueSerde    Serde to use for serializing the value
         * @param partitioner   the function used to determine how records are distributed among partitions of the topic,
         *                      if not specified and {@code keySerde} provides a {@link WindowedSerializer} for the key
         *                      {@link WindowedStreamPartitioner} will be used&mdash;otherwise {@link DefaultPartitioner}
         *                      will be used
         * @param           key type
         * @param           value type
         * @return  A new {@link Produced} instance configured with keySerde, valueSerde, and partitioner
         * @see KStream#through(string, Produced)
         * @see KStream#to(string, Produced)
         */
        public static Produced<K, V> With<K, V>(
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            IStreamPartitioner<K, V> partitioner)
        {
            return new Produced<K, V>(
                keySerde,
                valueSerde,
                partitioner,
                null);
        }

        /**
         * Create an instance of {@link Produced} with provided processor name.
         *
         * @param processorName the processor name to be used. If {@code null} a default processor name will be generated
         * @param         key type
         * @param         value type
         * @return a new instance of {@link Produced}
         */
        public static Produced<K, V> As<K, V>(string processorName)
        {
            return new Produced<K, V>(
                null,
                null,
                null,
                processorName);
        }

        /**
         * Create a Produced instance with provided keySerde.
         * @param keySerde      Serde to use for serializing the key
         * @param           key type
         * @param           value type
         * @return  A new {@link Produced} instance configured with keySerde
         * @see KStream#through(string, Produced)
         * @see KStream#to(string, Produced)
         */
        public static Produced<K, V> GetKeySerde<K, V>(ISerde<K> keySerde)
        {
            return new Produced<K, V>(
                keySerde, null, null, null);
        }

        /**
         * Create a Produced instance with provided valueSerde.
         * @param valueSerde    Serde to use for serializing the key
         * @param           key type
         * @param           value type
         * @return  A new {@link Produced} instance configured with valueSerde
         * @see KStream#through(string, Produced)
         * @see KStream#to(string, Produced)
         */
        public static Produced<K, V> GetValueSerde<K, V>(ISerde<V> valueSerde)
        {
            return new Produced<K, V>(
                null,
                valueSerde,
                null,
                null);
        }

        /**
         * Create a Produced instance with provided partitioner.
         * @param partitioner   the function used to determine how records are distributed among partitions of the topic,
         *                      if not specified and the key serde provides a {@link WindowedSerializer} for the key
         *                      {@link WindowedStreamPartitioner} will be used&mdash;otherwise {@link DefaultPartitioner} will be used
         * @param           key type
         * @param           value type
         * @return  A new {@link Produced} instance configured with partitioner
         * @see KStream#through(string, Produced)
         * @see KStream#to(string, Produced)
         */
        public static Produced<K, V> StreamPartitioner<K, V>(IStreamPartitioner<K, V> partitioner)
        {
            return new Produced<K, V>(null, null, partitioner, null);
        }

        /**
         * Create a Produced instance with provided keySerde and valueSerde.
         * @param keySerde      Serde to use for serializing the key
         * @param valueSerde    Serde to use for serializing the value
         * @param           key type
         * @param           value type
         * @return  A new {@link Produced} instance configured with keySerde and valueSerde
         * @see KStream#through(string, Produced)
         * @see KStream#to(string, Produced)
         */
        public static Produced<K, V> With<K, V>(
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            return new Produced<K, V>(
                keySerde,
                valueSerde,
                null,
                null);
        }
    }

    /**
     * This is used to provide the optional parameters when producing to new topics
     * using {@link KStream#through(string, Produced)} or {@link KStream#to(string, Produced)}.
     * @param key type
     * @param value type
     */
    public class Produced<K, V> : INamedOperation<Produced<K, V>>
    {
        public ISerde<K>? keySerde { get; private set; }
        public ISerde<V>? valueSerde { get; private set; }
        protected IStreamPartitioner<K, V> partitioner { get; private set; }
        protected string processorName { get; private set; }

        public Produced(
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            IStreamPartitioner<K, V> partitioner,
            string processorName)
        {
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.partitioner = partitioner;
            this.processorName = processorName;
        }

        protected Produced(Produced<K, V> produced)
        {
            if (produced is null)
            {
                throw new System.ArgumentNullException(nameof(produced));
            }

            this.keySerde = produced.keySerde;
            this.valueSerde = produced.valueSerde;
            this.partitioner = produced.partitioner;
            this.processorName = produced.processorName;
        }
        /**
         * Produce records using the provided partitioner.
         * @param partitioner   the function used to determine how records are distributed among partitions of the topic,
         *                      if not specified and the key serde provides a {@link WindowedSerializer} for the key
         *                      {@link WindowedStreamPartitioner} will be used&mdash;otherwise {@link DefaultPartitioner} wil be used
         * @return this
         */
        public Produced<K, V> WithStreamPartitioner(IStreamPartitioner<K, V> partitioner)
        {
            this.partitioner = partitioner;
            return this;
        }

        /**
         * Produce records using the provided valueSerde.
         * @param valueSerde    Serde to use for serializing the value
         * @return this
         */
        public Produced<K, V> WithValueSerde(ISerde<V>? valueSerde)
        {
            this.valueSerde = valueSerde;

            return this;
        }

        /**
         * Produce records using the provided keySerde.
         * @param keySerde    Serde to use for serializing the key
         * @return this
         */
        public Produced<K, V> WithKeySerde(ISerde<K>? keySerde)
        {
            this.keySerde = keySerde;

            return this;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            var produced = (Produced<object, object>)o;

            return keySerde.Equals(produced.keySerde)
                && valueSerde.Equals(produced.valueSerde)
                && partitioner.Equals(produced.partitioner);
        }

        public override int GetHashCode()
            => (this.keySerde, this.valueSerde, partitioner)
                .GetHashCode();

        public Produced<K, V> WithName(string name)
        {
            this.processorName = name;
            return this;
        }
    }
}
