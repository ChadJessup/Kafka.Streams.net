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
            return new SuppressedInternal<>(
                name,
                gracePeriod,
                bufferConfig,
                TimeDefinitions.WindowEndTimeDefinition.instance(),
                true
            );
        }


        public ISuppressed<K> withName(string name)
        {
            return new FinalResultsSuppressionBuilder<>(name, bufferConfig);
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
            FinalResultsSuppressionBuilder <object> that = (FinalResultsSuppressionBuilder <object>) o;
            return Objects.Equals(name, that.name) &&
                Objects.Equals(bufferConfig, that.bufferConfig);
        }


        public string name
        {
            return name;
        }


        public int hashCode()
        {
            return Objects.hash(name, bufferConfig);
        }


        public string ToString()
        {
            return "FinalResultsSuppressionBuilder{" +
                "name='" + name + '\'' +
                ", bufferConfig=" + bufferConfig +
                '}';
        }
    }
}