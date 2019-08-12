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
//namespace Kafka.Streams.KStream.Internals.Suppress
//{
//    public class EagerBufferConfigImpl : BufferConfigInternal<IEagerBufferConfig>, IEagerBufferConfig
//    {
//        public long maxRecords { get; }
//        public long maxBytes { get; }

//        public EagerBufferConfigImpl(long maxRecords, long maxBytes)
//        {
//            this.maxRecords = maxRecords;
//            this.maxBytes = maxBytes;
//        }

//        public IEagerBufferConfig withMaxRecords(long recordLimit)
//        {
//            return new EagerBufferConfigImpl(recordLimit, maxBytes);
//        }


//        public IEagerBufferConfig withMaxBytes(long byteLimit)
//        {
//            return new EagerBufferConfigImpl(maxRecords, byteLimit);
//        }

//        public override BufferFullStrategy bufferFullStrategy()
//        {
//            return BufferFullStrategy.EMIT;
//        }


//        public override bool Equals(object o)
//        {
//            if (this == o)
//            {
//                return true;
//            }
//            if (o == null || GetType() != o.GetType())
//            {
//                return false;
//            }

//            EagerBufferConfigImpl that = (EagerBufferConfigImpl)o;
//            return maxRecords == that.maxRecords &&
//                maxBytes == that.maxBytes;
//        }


//        public override int GetHashCode()
//        {
//            return (maxRecords, maxBytes).GetHashCode();
//        }


//        public override string ToString()
//        {
//            return "EagerBufferConfigImpl{maxRecords=" + maxRecords + ", maxBytes=" + maxBytes + '}';
//        }
//    }
//}