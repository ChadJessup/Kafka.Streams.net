
//        public byte[] rawValue(V value)
//        {
//            try
//            {
//                return valueSerde.Serializer.Serialize(topic, value);
//            }
//            catch (InvalidCastException e)
//            {
//                string valueClass;
//                Type serializerClass;
//                if (valueSerializer() is ValueAndTimestampSerializer)
//                {
//                    serializerClass = ((ValueAndTimestampSerializer)valueSerializer()).valueSerializer.GetType();
//                    valueClass = value == null ? "unknown because value is null" : ((ValueAndTimestamp)value).value().GetType().getName();
//                }
//                else
//                {
//                    serializerClass = valueSerializer().GetType();
//                    valueClass = value == null ? "unknown because value is null" : value.GetType().FullName;
//                }
//                throw new StreamsException(
//                        string.Format("A serializer (%s) is not compatible to the actual value type " +
//                                        "(value type: %s). Change the default Serdes in StreamConfig or " +
//                                        "provide correct Serdes via method parameters.",
//                                serializerClass.FullName,
//                                valueClass),
//                        e);
//            }
//        }
//    }
//}