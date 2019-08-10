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

using Kafka.Streams.Interfaces;
using Kafka.Streams.IProcessor.Interfaces;

namespace Kafka.Streams.IProcessor.Internals
{
    public class Sink<K, V> : AbstractNode, ISink<K, V>
    {
        private ITopicNameExtractor<K, V> _topicNameExtractor;

        public Sink(
            string name,
            ITopicNameExtractor<K, V> topicNameExtractor)
            : base(name)
        {
            this._topicNameExtractor = topicNameExtractor;
        }

        public Sink(
            string name,
            string topic)
            : base(name)
        {
            this._topicNameExtractor = new StaticTopicNameExtractor<K, V>(topic);
        }

        public string Topic
        {
            get
            {
                if (_topicNameExtractor is StaticTopicNameExtractor<K, V>)
                {
                    return ((StaticTopicNameExtractor<K, V>)_topicNameExtractor).topicName;
                }
                else
                {
                    return null;
                }
            }
        }

        public ITopicNameExtractor<K, V> topicNameExtractor()
        {
            return this._topicNameExtractor;
        }
    }
}