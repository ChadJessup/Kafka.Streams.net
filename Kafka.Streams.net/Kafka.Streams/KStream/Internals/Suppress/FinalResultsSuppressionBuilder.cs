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
using Kafka.Common;
using Kafka.Streams.KStream.Interfaces;
using System;

namespace Kafka.Streams.KStream.Internals.Suppress
{






    public class FinalResultsSuppressionBuilder<K> : ISuppressed<K>, INamedSuppressed<K>
        where K : Windowed<K>
    {
        private string name;
        private IStrictBufferConfig bufferConfig;

        public FinalResultsSuppressionBuilder(string name, IStrictBufferConfig bufferConfig)
        {
            this.name = name;
            this.bufferConfig = bufferConfig;
        }

        public SuppressedInternal<K> buildFinalResultsSuppression(TimeSpan gracePeriod)
        {
            return new SuppressedInternal<K>(
                name,
                gracePeriod,
                bufferConfig,
                ITimeDefinition.WindowEndTimeDefinition.instance(),
                true
            );
        }


        public ISuppressed<K> withName(string name)
        {
            return new FinalResultsSuppressionBuilder<K>(name, bufferConfig);
        }


        public bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            FinalResultsSuppressionBuilder<K> that = (FinalResultsSuppressionBuilder<K>) o;
            return name.Equals(that.name) &&
                bufferConfig.Equals(that.bufferConfig);
        }

        public int hashCode()
        {
            return (name, bufferConfig).GetHashCode();
        }

        public override string ToString()
        {
            return "FinalResultsSuppressionBuilder{" +
                "name='" + name + '\'' +
                ", bufferConfig=" + bufferConfig +
                '}';
        }
    }
}