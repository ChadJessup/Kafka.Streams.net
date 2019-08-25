/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processor.Interfaces;

namespace Kafka.Streams.KStream
{
    /**
     * This is used to provide the optional parameters when producing to new topics
     * using {@link KStream#through(string, Produced)} or {@link KStream#to(string, Produced)}.
     * @param key type
     * @param value type
     */
    public class Produced<K, V> : INamedOperation<Produced<K, V>>
    {
        public ISerde<K> keySerde { get; private set; }
        public ISerde<V> valueSerde { get; private set; }
        protected IStreamPartitioner<K, V> partitioner;
        protected string processorName;

        private Produced(
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
            this.keySerde = produced.keySerde;
            this.valueSerde = produced.valueSerde;
            this.partitioner = produced.partitioner;
            this.processorName = produced.processorName;
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
        public static Produced<K, V> with(
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            return new Produced<K, V>(
                keySerde,
                valueSerde,
                null,
                null);
        }

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
        public static Produced<K, V> with(
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
        public static Produced<K, V> As(string processorName)
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
        public static Produced<K, V> GetKeySerde(ISerde<K> keySerde)
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
        public static Produced<K, V> GetValueSerde(ISerde<V> valueSerde)
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
        public static Produced<K, V> streamPartitioner(IStreamPartitioner<K, V> partitioner)
        {
            return new Produced<K, V>(null, null, partitioner, null);
        }

        /**
         * Produce records using the provided partitioner.
         * @param partitioner   the function used to determine how records are distributed among partitions of the topic,
         *                      if not specified and the key serde provides a {@link WindowedSerializer} for the key
         *                      {@link WindowedStreamPartitioner} will be used&mdash;otherwise {@link DefaultPartitioner} wil be used
         * @return this
         */
        public Produced<K, V> withStreamPartitioner(IStreamPartitioner<K, V> partitioner)
        {
            this.partitioner = partitioner;
            return this;
        }

        /**
         * Produce records using the provided valueSerde.
         * @param valueSerde    Serde to use for serializing the value
         * @return this
         */
        public Produced<K, V> withValueSerde(ISerde<V> valueSerde)
        {
            this.valueSerde = valueSerde;

            return this;
        }

        /**
         * Produce records using the provided keySerde.
         * @param keySerde    Serde to use for serializing the key
         * @return this
         */
        public Produced<K, V> withKeySerde(ISerde<K> keySerde)
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

        public Produced<K, V> withName(string name)
        {
            this.processorName = name;
            return this;
        }
    }
}