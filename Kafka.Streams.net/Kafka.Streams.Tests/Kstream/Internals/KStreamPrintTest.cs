using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals.Assignments;
using Moq;
using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KStreamPrintTest
    {

        private readonly Stream byteOutStream;
        private readonly IKeyValueProcessor<int, string> printProcessor;

        public KStreamPrintTest()
        {
            byteOutStream = new MemoryStream();

            KStreamPrint<int, string> kStreamPrint = new KStreamPrint<int, string>(
                //byteOutStream,
                (key, value) => string.Format("%d, %s", key, value));//,
                //"test-stream");

            printProcessor = kStreamPrint.Get();
            IProcessorContext processorContext = Mock.Of<IProcessorContext>();
            //EasyMock.replay(processorContext);

            printProcessor.Init(processorContext);
        }

        [Fact]

        public void testPrintStreamWithProvidedKeyValueMapper()
        {
            List<KeyValuePair<int, string>> inputRecords = new List<KeyValuePair<int, string>>
            {
                    KeyValuePair.Create(0, "zero"),
                    KeyValuePair.Create(1, "one"),
                    KeyValuePair.Create(2, "two"),
                    KeyValuePair.Create(3, "three"),
            };

            string[] expectedResult = {
            "[test-stream]: 0, zero",
            "[test-stream]: 1, one",
            "[test-stream]: 2, two",
            "[test-stream]: 3, three"};

            foreach (KeyValuePair<int, string> record in inputRecords)
            {
                printProcessor.Process(record.Key, record.Value);
            }
            printProcessor.Close();

            string[] flushOutData = new string[0];// byteOutStream.ToByteArray()).Split("\\r*\\n");
            for (var i = 0; i < flushOutData.Length; i++)
            {
                Assert.Equal(expectedResult[i], flushOutData[i]);
            }
        }
    }
}
