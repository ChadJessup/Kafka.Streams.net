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
//using Kafka.Streams.Processor;
//using Kafka.Streams.Processor.Interfaces;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamFlatTransformProcessor<KIn, VIn, KOut, VOut> : AbstractProcessor<KIn, VIn>
//    {
//        private ITransformer<KIn, VIn, IEnumerable<KeyValue<KOut, VOut>>> transformer;

//        public KStreamFlatTransformProcessor(ITransformer<KIn, VIn, IEnumerable<KeyValue<KOut, VOut>>> transformer)
//        {
//            this.transformer = transformer;
//        }


//        public void init(IProcessorContext<K, V> context)
//        {
//            base.init(context);
//            transformer.init(context);
//        }


//        public override void process(KIn key, VIn value)
//        {
//            IEnumerable<KeyValue<KOut, VOut>> pairs = transformer.transform(key, value);
//            if (pairs != null)
//            {
//                foreach (KeyValue<KOut, VOut> pair in pairs)
//                {
//                    //context.forward(pair.key, pair.value);
//                }
//            }
//        }


//        public override void close()
//        {
//            transformer.close();
//        }
//    }
//}
