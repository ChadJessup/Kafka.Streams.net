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

//            printProcessor = kStreamPrint.Get();
//            IProcessorContext processorContext = EasyMock.createNiceMock(typeof(IProcessorContext));
//            EasyMock.replay(processorContext);

//            printProcessor.Init(processorContext);
//        }

//        [Fact]

//        public void testPrintStreamWithProvidedKeyValueMapper()
//        {
//            List<KeyValuePair<int, string>> inputRecords = Array.AsReadOnly(
//                    KeyValuePair.Create(0, "zero"),
//                    KeyValuePair.Create(1, "one"),
//                    KeyValuePair.Create(2, "two"),
//                    KeyValuePair.Create(3, "three"));

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

//            string[] flushOutData = new string(byteOutStream.toByteArray(), StandardCharsets.UTF_8).Split("\\r*\\n");
//            for (var i = 0; i < flushOutData.Length; i++)
//            {
//                Assert.Equal(expectedResult[i], flushOutData[i]);
//            }
//        }
//    }
//}
