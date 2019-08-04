using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public KStreamFlatMap<K, V, K1, V1> : ProcessorSupplier<K, V>
    {
        private IKeyValueMapper<K,V, KeyValuePair<K1, V1>> mapper;

        KStreamFlatMap(IKeyValueMapper<K, V, Iterable<KeyValue<K1, V1>>> mapper)
{
            this.mapper = mapper;
        }


        public Processor<K, V> get()
{
            return new KStreamFlatMapProcessor();
        }

        private KStreamFlatMapProcessor : AbstractProcessor<K, V> {

            public void process(K key, V value)
{
                foreach (KeyValue<K1, V1> newPair in mapper.apply(key, value))
{
                    context().forward(newPair.key, newPair.value);
                }
            }
        }
    }
}