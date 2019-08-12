﻿/*
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

namespace Kafka.Streams.KStream.Internals
{
    public class KTableKTableLeftJoinValueGetter : IKTableValueGetter<K, R>
    {

        private IKTableValueGetter<K, V1> valueGetter1;
        private IKTableValueGetter<K, V2> valueGetter2;

        KTableKTableLeftJoinValueGetter(IKTableValueGetter<K, V1> valueGetter1,
                                         IKTableValueGetter<K, V2> valueGetter2)
        {
            this.valueGetter1 = valueGetter1;
            this.valueGetter2 = valueGetter2;
        }


        public void init(IProcessorContext<K, V> context)
        {
            valueGetter1.init(context);
            valueGetter2.init(context);
        }


        public ValueAndTimestamp<R> get(K key)
        {
            ValueAndTimestamp<V1> valueAndTimestamp1 = valueGetter1[key];
            V1 value1 = getValueOrNull(valueAndTimestamp1);

            if (value1 != null)
            {
                ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter2[key];
                V2 value2 = getValueOrNull(valueAndTimestamp2);
                long resultTimestamp;
                if (valueAndTimestamp2 == null)
                {
                    resultTimestamp = valueAndTimestamp1.timestamp();
                }
                else
                {

                    resultTimestamp = Math.Max(valueAndTimestamp1.timestamp(), valueAndTimestamp2.timestamp());
                }
                return ValueAndTimestamp.make(joiner.apply(value1, value2), resultTimestamp);
            }
            else
            {

                return null;
            }
        }


        public void close()
        {
            valueGetter1.close();
            valueGetter2.close();
        }
    }
}