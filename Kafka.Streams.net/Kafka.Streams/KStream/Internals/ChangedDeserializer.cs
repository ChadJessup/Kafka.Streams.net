///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for.Additional information regarding copyright ownership.
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
//using Confluent.Kafka;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class ChangedDeserializer<T> : IDeserializer<Change<T>>
//    {

//        private static int NEWFLAG_SIZE = 1;

//        private IDeserializer<T> inner;

//        public ChangedDeserializer(IDeserializer<T> inner)
//        {
//            this.inner = inner;
//        }

//        public IDeserializer<T> inner()
//        {
//            return inner;
//        }

//        public void setInner(IDeserializer<T> inner)
//        {
//            this.inner = inner;
//        }


//        public Change<T> deserialize(string topic, Headers headers, byte[] data)
//        {

//            byte[] bytes = new byte[data.Length - NEWFLAG_SIZE];

//            System.arraycopy(data, 0, bytes, 0, bytes.Length);

//            if (ByteBuffer.wrap(data)[data.Length - NEWFLAG_SIZE] != 0)
//            {
//                return new Change<>(inner.Deserialize(topic, headers, bytes), null);
//            }
//            else
//            {

//                return new Change<>(null, inner.Deserialize(topic, headers, bytes));
//            }
//        }


//        public Change<T> deserialize(string topic, byte[] data)
//        {
//            return deserialize(topic, null, data);
//        }


//        public void close()
//        {
//            inner.close();
//        }
//    }
//}