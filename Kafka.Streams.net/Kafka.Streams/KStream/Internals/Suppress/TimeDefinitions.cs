using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.KStream.Interfaces;

namespace Kafka.Streams.KStream.Internals.Suppress
{

    public class RecordTimeDefintion<K, V> : ITimeDefinition<K, V>
    {
        private static readonly RecordTimeDefintion<K, V> INSTANCE = new RecordTimeDefintion<K, V>();

        private RecordTimeDefintion() { }


        public static RecordTimeDefintion<K, V> instance()
        {
            return RecordTimeDefintion<K, V>.INSTANCE;
        }

        public long time(IProcessorContext context, K key)
        {
            return context.timestamp;
        }

        public TimeDefinitionType type()
        {
            return TimeDefinitionType.RECORD_TIME;
        }
    }
}