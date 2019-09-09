/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processor;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamWindowAggregate<K, V, Agg, W> : IKStreamAggProcessorSupplier<K, Windowed<K>, V, Agg>
        where W : Window
    {
        private ILogger log = new LoggerFactory().CreateLogger<KStreamWindowAggregate<K, V, Agg, W>>();

        private string storeName;
        private Windows<W> windows;
        private IInitializer<Agg> initializer;
        private IAggregator<K, V, Agg> aggregator;

        private bool sendOldValues = false;

        public KStreamWindowAggregate(
            Windows<W> windows,
            string storeName,
            IInitializer<Agg> initializer,
            IAggregator<K, V, Agg> aggregator)
        {
            this.windows = windows;
            this.storeName = storeName;
            this.initializer = initializer;
            this.aggregator = aggregator;
        }


        public IProcessor<K, V> get()
        {
            return null;// new KStreamWindowAggregateProcessor();
        }

        public void enableSendingOldValues()
        {
            sendOldValues = true;
        }


        public IKTableValueGetterSupplier<Windowed<K>, Agg> view()
        {
            return null;
            //return new KTableValueGetterSupplier<Windowed<K>, Agg>()
            //{

            //    public KTableValueGetter<Windowed<K>, Agg> get()
            //{
            //                return new KStreamWindowAggregateValueGetter();
            //            }


            //            public string[] storeNames()
            //{
            //                return new string[] {storeName};
            //            }
            //        };
        }

        public IProcessorSupplier<Windowed<K>, Agg> GetSwappedProcessorSupplier()
        {
            throw new System.NotImplementedException();
        }
    }
}