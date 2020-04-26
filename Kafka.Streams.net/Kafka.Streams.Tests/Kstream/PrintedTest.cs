//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Internals.Assignmentss;
//using Kafka.Streams.Tests.Helpers;
//using System;
//using System.IO;
//using Xunit;
//namespace Kafka.Streams.Tests.Kstream
//{


//    public class PrintedTest
//    {

//        private TextWriter originalSysOut = System.Console.Out;
//        private ByteArrayOutputStream sysOut = new ByteArrayOutputStream();
//        private Printed<string, int> sysOutPrinter;


//        public void before()
//        {
//            System.Console.SetOut(new PrintStream(sysOut));
//            sysOutPrinter = Printed.toSysOut();
//        }


//        public void after()
//        {
//            System.Console.SetOut(originalSysOut);
//        }

//        [Fact]
//        public void shouldCreateProcessorThatPrintsToFile()// throws IOException

//        {
//            FileInfo file = TestUtils.tempFile();
//            IProcessorSupplier<string, int> processorSupplier = new PrintedInternal<>(
//                    Printed.toFile<string, int>(file.getPath()))
//                    .Build("processor");
//            IKeyValueProcessor<string, int> processor = processorSupplier.Get();
//            processor.Process("hi", 1);
//            processor.Close();
//            try
//            {
//                InputStream stream = Files.newInputStream(file.toPath());
//                var data = new byte[stream.available()];
//                stream.read(data);
//                Assert.Equal(data, "[processor]: hi, 1\n");
//            }
//            catch (Exception e)
//            {
//            }
//        }

//        [Fact]
//        public void shouldCreateProcessorThatPrintsToStdOut()// throws UnsupportedEncodingException

//        {
//            IProcessorSupplier<string, int> supplier = new PrintedInternal<>(sysOutPrinter).Build("processor");
//            var processor = supplier.Get();

//            processor.Process("good", 2);
//            processor.Close();
//            Assert.Equal(sysOut.ToString(StandardCharsets.UTF_8.Name()), "[processor]: good, 2\n");
//        }

//        [Fact]
//        public void shouldPrintWithLabel()// throws UnsupportedEncodingException

//        {
//            IKeyValueProcessor<string, int> processor = new PrintedInternal<>(sysOutPrinter.withLabel("label"))
//                .Build("processor")
//                .Get();

//            processor.Process("hello", 3);
//            processor.Close();
//            Assert.Equal(sysOut.ToString(StandardCharsets.UTF_8.Name()), "[label]: hello, 3\n");
//        }

//        [Fact]
//        public void shouldPrintWithKeyValueMapper()// throws UnsupportedEncodingException

//        {
//            Processor<string, int> processor = new PrintedInternal<>(sysOutPrinter.withKeyValueMapper(
//                        new KeyValueMapper<string, int, string>()));
//            //{


//            //public string apply(string key, int value)
//            //{
//            //return string.Format("%s => %d", key, value);
//            //}
//            //})).Build("processor")
//            //.Get();
//            processor.Process("hello", 1);
//            processor.Close();
//            Assert.Equal(sysOut.ToString(StandardCharsets.UTF_8.Name()), "[processor]: hello => 1\n");
//        }

//        [Fact]
//        public void shouldThrowNullPointerExceptionIfFilePathIsNull()
//        {
//            Printed.toFile(null);
//        }

//        [Fact]
//        public void shouldThrowNullPointerExceptionIfMapperIsNull()
//        {
//            sysOutPrinter.withKeyValueMapper(null);
//        }

//        [Fact]
//        public void shouldThrowNullPointerExceptionIfLabelIsNull()
//        {
//            sysOutPrinter.WithLabel(null);
//        }

//        [Fact]
//        public void shouldThrowTopologyExceptionIfFilePathIsEmpty()
//        {
//            Printed.toFile("");
//        }

//        [Fact]
//        public void shouldThrowTopologyExceptionIfFilePathDoesntExist()
//        {
//            Printed.toFile("/this/should/not/exist");
//        }
//    }
//}
