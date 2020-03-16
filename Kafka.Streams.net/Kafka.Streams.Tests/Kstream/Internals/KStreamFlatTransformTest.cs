///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//using Kafka.Streams.KStream.Internals;
//using System;

//namespace Kafka.Streams.KStream.Internals
//{

















//    public class KStreamFlatTransformTest : EasyMockSupport
//    {

//        private int inputKey;
//        private int inputValue;

//        private Transformer<int, int, Iterable<KeyValue<int, int>>> transformer;
//        private IProcessorContext context;

//        private KStreamFlatTransformProcessor<int, int, int, int> processor;


//        public void setUp()
//        {
//            inputKey = 1;
//            inputValue = 10;
//            transformer = mock(Transformer));
//            context = strictMock(IProcessorContext));
//            processor = new KStreamFlatTransformProcessor<>(transformer);
//        }

//        [Fact]
//        public void shouldInitialiseFlatTransformProcessor()
//        {
//            transformer.init(context);
//            replayAll();

//            processor.init(context);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldTransformInputRecordToMultipleOutputRecords()
//        {
//            Iterable<KeyValue<int, int>> outputRecords = Array.AsReadOnly(
//                    KeyValue.Pair(2, 20),
//                    KeyValue.Pair(3, 30),
//                    KeyValue.Pair(4, 40));
//            processor.init(context);
//            EasyMock.reset(transformer);

//            EasyMock.expect(transformer.transform(inputKey, inputValue)).andReturn(outputRecords);
//            foreach (KeyValue<int, int> outputRecord in outputRecords)
//            {
//                context.forward(outputRecord.key, outputRecord.value);
//            }
//            replayAll();

//            processor.process(inputKey, inputValue);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldAllowEmptyListAsResultOfTransform()
//        {
//            processor.init(context);
//            EasyMock.reset(transformer);

//            EasyMock.expect(transformer.transform(inputKey, inputValue))
//                .andReturn(Collections.< KeyValue<int, int> > emptyList());
//            replayAll();

//            processor.process(inputKey, inputValue);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldAllowNullAsResultOfTransform()
//        {
//            processor.init(context);
//            EasyMock.reset(transformer);

//            EasyMock.expect(transformer.transform(inputKey, inputValue))
//                .andReturn(null);
//            replayAll();

//            processor.process(inputKey, inputValue);

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldCloseFlatTransformProcessor()
//        {
//            transformer.close();
//            replayAll();

//            processor.close();

//            //verifyAll();
//        }

//        [Fact]
//        public void shouldGetFlatTransformProcessor()
//        {
//            TransformerSupplier<int, int, Iterable<KeyValue<int, int>>> transformerSupplier =
//        mock(TransformerSupplier));
//            KStreamFlatTransform<int, int, int, int> processorSupplier =
//                new KStreamFlatTransform<>(transformerSupplier);

//            EasyMock.expect(transformerSupplier.get()).andReturn(transformer);
//            replayAll();

//            Processor<int, int> processor = processorSupplier.get();

//            //verifyAll();
//            Assert.True(processor is KStreamFlatTransformProcessor);
//        }
//    }
//}
