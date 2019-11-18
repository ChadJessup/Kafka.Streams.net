

//        public long onInvalidTimestamp(ConsumeResult<object, object> record,
//                                       long recordTimestamp,
//                                       long partitionTime)
//        {
//            if (partitionTime < 0)
//            {
//                throw new StreamsException("Could not infer new timestamp for input record " + record
//                        + " because partition time is unknown.");
//            }
//            return partitionTime;
//        }


//    }
//}