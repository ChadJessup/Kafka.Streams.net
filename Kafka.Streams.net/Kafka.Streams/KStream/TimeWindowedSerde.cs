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
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream.Internals;

//namespace Kafka.Streams.KStream
//{
//    public class TimeWindowedSerde<T> : WrapperSerde<Windowed<T>>
//    {
//        // Default constructor needed for reflection object creation

//        public TimeWindowedSerde()
//            : base(new TimeWindowedSerializer<>(), new TimeWindowedDeserializer<>())
//        {
//        }

//        public TimeWindowedSerde(Serde<T> inner)
//            : base(new TimeWindowedSerializer<>(inner.serializer()), new TimeWindowedDeserializer<>(inner.deserializer()))
//        {
//        }

//        // This constructor can be used for serialize/deserialize a windowed topic
//        public TimeWindowedSerde(ISerde<T> inner, long windowSize)
//            : base(new TimeWindowedSerializer<T>(inner.serializer()), new TimeWindowedDeserializer<>(inner.deserializer(), windowSize))
//        {
//        }

//        // Helper method for users to specify whether the input topic is a changelog topic for deserializing the key properly.
//        public TimeWindowedSerde<T> forChangelog(bool isChangelogTopic)
//        {
//            TimeWindowedDeserializer<T> deserializer = (TimeWindowedDeserializer<T>)this.deserializer;
//            deserializer.setIsChangelogTopic(isChangelogTopic);
//            return this;
//        }
//    }
//}
