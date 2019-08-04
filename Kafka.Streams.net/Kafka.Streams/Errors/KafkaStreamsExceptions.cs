﻿using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Kafka.Streams.Errors
{
    public KafkaStreamsExceptions : Exception
    {
        public KafkaStreamsExceptions()
        {
        }

        public KafkaStreamsExceptions(string message)
            : base(message)
        {
        }

        public KafkaStreamsExceptions(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        protected KafkaStreamsExceptions(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
