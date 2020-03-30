namespace Kafka.Streams.Tests.Kstream.Internals
{
}
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals.Assignmentss;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{

//    public class KStreamPrintTest
//    {

//        private ByteArrayOutputStream byteOutStream;
//        private Processor<int, string> printProcessor;

//        public void setUp()
//        {
//            byteOutStream = new ByteArrayOutputStream();

//            KStreamPrint<int, string> kStreamPrint = new KStreamPrint<int, string>(new PrintForeachAction<>(
//                byteOutStream,
//                (key, value) => string.Format("%d, %s", key, value),
//                "test-stream"));

//            printProcessor = kStreamPrint.get();
//            IProcessorContext processorContext = EasyMock.createNiceMock(typeof(IProcessorContext));
//            EasyMock.replay(processorContext);

//            printProcessor.init(processorContext);
//        }

//        [Fact]

//        public void testPrintStreamWithProvidedKeyValueMapper()
//        {
//            List<KeyValuePair<int, string>> inputRecords = Array.AsReadOnly(
//                    new KeyValuePair<>(0, "zero"),
//                    new KeyValuePair<>(1, "one"),
//                    new KeyValuePair<>(2, "two"),
//                    new KeyValuePair<>(3, "three"));

//            string[] expectedResult = {
//            "[test-stream]: 0, zero",
//            "[test-stream]: 1, one",
//            "[test-stream]: 2, two",
//            "[test-stream]: 3, three"};

//            foreach (KeyValuePair<int, string> record in inputRecords)
//            {
//                printProcessor.process(record.key, record.value);
//            }
//            printProcessor.close();

//            string[] flushOutData = new string(byteOutStream.toByteArray(), StandardCharsets.UTF_8).split("\\r*\\n");
//            for (var i = 0; i < flushOutData.Length; i++)
//            {
//                Assert.Equal(expectedResult[i], flushOutData[i]);
//            }
//        }
//    }
//}
