//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Temp;
//using Xunit;

//namespace Kafka.Streams.Tests
//{

//    public class PrintedTest
//    {

//        private PrintStream originalSysOut = System.Console.Out;
//        private ByteArrayOutputStream sysOut = new ByteArrayOutputStream();
//        private Printed<string, int> sysOutPrinter;


//        public void before()
//        {
//            System.setOut(new PrintStream(sysOut));
//            sysOutPrinter = Printed.toSysOut();
//        }


//        public void after()
//        {
//            System.setOut(originalSysOut);
//        }

//        [Fact]
//        public void shouldCreateProcessorThatPrintsToFile()// throws IOException

//        {
//            File file = TestUtils.tempFile();
//            ProcessorSupplier<string, int> processorSupplier = new PrintedInternal<>(
//                    Printed.< string, int > toFile(file.getPath()))
//                    .Build("processor");
//            Processor<string, int> processor = processorSupplier.get();
//            processor.process("hi", 1);
//            processor.close();
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
//            ProcessorSupplier<string, int> supplier = new PrintedInternal<>(sysOutPrinter).Build("processor");
//            Processor<string, int> processor = supplier.get();

//            processor.process("good", 2);
//            processor.close();
//            Assert.Equal(sysOut.ToString(StandardCharsets.UTF_8.name()), equalTo("[processor]: good, 2\n"));
//        }

//        [Fact]
//        public void shouldPrintWithLabel()// throws UnsupportedEncodingException

//        {
//            Processor<string, int> processor = new PrintedInternal<>(sysOutPrinter.withLabel("label"))
//                        .Build("processor")
//                        .get();

//            processor.process("hello", 3);
//            processor.close();
//            Assert.Equal(sysOut.ToString(StandardCharsets.UTF_8.name()), equalTo("[label]: hello, 3\n"));
//        }

//        [Fact]
//        public void shouldPrintWithKeyValueMapper()// throws UnsupportedEncodingException

//        {
//            Processor<string, int> processor = new PrintedInternal<>(sysOutPrinter.withKeyValueMapper(
//                        new IKeyValueMapper<string, int, string>()));
//            //{


//            //public string apply(string key, int value)
//            //{
//            //return string.Format("%s => %d", key, value);
//            //}
//            //})).Build("processor")
//            //.get();
//            processor.process("hello", 1);
//            processor.close();
//            Assert.Equal(sysOut.ToString(StandardCharsets.UTF_8.name()), equalTo("[processor]: hello => 1\n"));
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
//            sysOutPrinter.withLabel(null);
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