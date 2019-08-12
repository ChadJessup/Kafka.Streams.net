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
using Kafka.Streams.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public class Source : AbstractNode, ISource
    {
        private HashSet<string> topics;
        private Regex topicPattern;

        public Source(
            string name,
            HashSet<string> topics,
            Regex pattern)
            : base(name)
        {
            if (topics == null && pattern == null)
            {
                throw new System.ArgumentException("Either topics or pattern must be not-null, but both are null.");
            }
            if (topics != null && pattern != null)
            {
                throw new System.ArgumentException("Either topics or pattern must be null, but both are not null.");
            }

            this.topics = topics;
            this.topicPattern = pattern;
        }

        public HashSet<string> topicSet()
        {
            return topics;
        }


        public override void addPredecessor(INode predecessor)
        {
            throw new InvalidOperationException("Sources don't have predecessors.");
        }


        public string ToString()
        {
            string topicsString = topics == null ? topicPattern.ToString() : topics.ToString();

            return "Source: " + name + " (topics: " + topicsString + ")\n      -=> " + nodeNames(successors);
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

            Source source = (Source)o;
            // omit successor to avoid infinite loops
            return name.Equals(source.name)
                && topics.Equals(source.topics)
                && (topicPattern == null ? source.topicPattern == null :
                    topicPattern.pattern().Equals(source.topicPattern.pattern()));
        }


        public override int GetHashCode()
        {
            // omit successor as it might change and alter the hash code
            return (name, topics, topicPattern).GetHashCode();
        }
    }
}
