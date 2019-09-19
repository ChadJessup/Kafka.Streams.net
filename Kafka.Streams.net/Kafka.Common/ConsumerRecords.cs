using Confluent.Kafka;
using System.Collections.Generic;

namespace Kafka.Common
{
    public class ConsumerRecord
    {
        public static long NO_TIMESTAMP = -1L;
    }

    public class ConsumerRecords<T1, T2> : List<ConsumeResult<T1, T2>>
    {
    }
}