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
//using Kafka.Streams.Kafka.Streams;

//namespace Kafka.Streams.KStream.Internals
//{














//    public class KTableKTableRightJoinTest
//    {
//        [Fact]
//        public void shouldLogAndMeterSkippedRecordsDueToNullLeftKey()
//        {
//            var builder = new StreamsBuilder();


//            Processor<string, Change<string>> join = new KTableKTableRightJoin<>(
//                (IKTable<string, string, string>)builder.Table("left", Consumed.with(Serdes.String(), Serdes.String())),
//                (IKTable<string, string, string>)builder.Table("right", Consumed.with(Serdes.String(), Serdes.String())),
//                null
//            ).get();

//            var context = new MockProcessorContext();
//            context.setRecordMetadata("left", -1, -2, null, -3);
//            join.init(context);
//            LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
//            join.process(null, new Change<>("new", "old"));
//            LogCaptureAppender.unregister(appender);

//            Assert.Equal(1.0, getMetricByName(context.metrics().metrics(), "skipped-records-total", "stream-metrics").metricValue());
//            Assert.Equal(appender.getMessages(), asItem("Skipping record due to null key. change=[(new<-old)] topic=[left] partition=[-1] offset=[-2]"));
//        }
//    }
//}
