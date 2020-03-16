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

//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State.KeyValues;

//namespace Kafka.Streams.KStream.Internals
//{

















//    public class MaterializedInternalTest
//    {

//        @Mock(type = MockType.NICE)
//        private InternalNameProvider nameProvider;

//        @Mock(type = MockType.NICE)
//        private KeyValueBytesStoreSupplier supplier;
//        private string prefix = "prefix";

//        [Fact]
//        public void shouldGenerateStoreNameWithPrefixIfProvidedNameIsNull()
//        {
//            var generatedName = prefix + "-store";
//            EasyMock.expect(nameProvider.newStoreName(prefix)).andReturn(generatedName);

//            EasyMock.replay(nameProvider);

//            MaterializedInternal<object, object, StateStore> materialized =
//                new MaterializedInternal<>(Materialized.with(null, null), nameProvider, prefix);

//            Assert.Equal(materialized.StoreName, equalTo(generatedName));
//            EasyMock.verify(nameProvider);
//        }

//        [Fact]
//        public void shouldUseProvidedStoreNameWhenSet()
//        {
//            var storeName = "store-name";
//            MaterializedInternal<object, object, StateStore> materialized =
//                new MaterializedInternal<>(Materialized.As(storeName), nameProvider, prefix);
//            Assert.Equal(materialized.StoreName, equalTo(storeName));
//        }

//        [Fact]
//        public void shouldUseStoreNameOfSupplierWhenProvided()
//        {
//            var storeName = "other-store-name";
//            EasyMock.expect(supplier.name()).andReturn(storeName).anyTimes();
//            EasyMock.replay(supplier);
//            MaterializedInternal<object, object, IKeyValueStore<Bytes, byte[]>> materialized =
//                new MaterializedInternal<>(Materialized.As(supplier), nameProvider, prefix);
//            Assert.Equal(materialized.StoreName, equalTo(storeName));
//        }
//    }
//}
