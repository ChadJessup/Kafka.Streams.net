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
using Kafka.Streams.Processor;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public partial class KTableKTableRightJoin<K, R, V1, V2> : KTableKTableAbstractJoin<K, R, V1, V2>
    {
        private static ILogger LOG = new LoggerFactory().CreateLogger<KTableKTableRightJoin<K, R, V1, V2>>();

        public KTableKTableRightJoin(
            KTableImpl<K, object, V1> table1,
            KTableImpl<K, object, V2> table2,
            IValueJoiner<V1, V2, R> joiner)
            : base(table1, table2, joiner)
        {
        }

        public IProcessor<K, Change<V1>> get()
        {
            return new KTableKTableRightJoinProcessor(valueGetterSupplier2());
        }

        public IKTableValueGetterSupplier<K, R> view()
        {
            return new KTableKTableRightJoinValueGetterSupplier(valueGetterSupplier1, valueGetterSupplier2);
        }

        private class KTableKTableRightJoinValueGetterSupplier : KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2>
        {
            public KTableKTableRightJoinValueGetterSupplier(
                IKTableValueGetterSupplier<K, V1> valueGetterSupplier1,
                IKTableValueGetterSupplier<K, V2> valueGetterSupplier2)
                : base(valueGetterSupplier1, valueGetterSupplier2)
            {
            }

            public IKTableValueGetter<K, R> get()
            {
                return new KTableKTableRightJoinValueGetter(valueGetterSupplier1(), valueGetterSupplier2());
            }
        }
    }
}