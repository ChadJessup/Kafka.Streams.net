
//    public class LogAndFailExceptionHandler : IDeserializationExceptionHandler
//    {

//        private static ILogger log = new LoggerFactory().CreateLogger<LogAndFailExceptionHandler>();


//        public DeserializationHandlerResponse handle(IProcessorContext context,
//                                                      ConsumeResult<byte[], byte[]> record,
//                                                      Exception exception)
//        {

//            log.LogError("Exception caught during Deserialization, " +
//                      "taskId: {}, topic: {}, partition: {}, offset: {}",
//                      context.taskId(), record.Topic, record.Partition, record.offset(),
//                      exception);

//            return DeserializationHandlerResponse.FAIL;
//        }


//        public void configure(Dictionary<string, object> configs)
//        {
//            // ignore
//        }
//    }
//}