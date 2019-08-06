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
using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.KStream.Internals.Graph;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    private class SourceNodeFactory : NodeFactory
    {

        private List<string> topics;
        private Pattern pattern;
        private IDeserializer<object> keyDeserializer;
        private IDeserializer<object> valDeserializer;
        private TimestampExtractor timestampExtractor;

        private SourceNodeFactory(string name,
                                  string[] topics,
                                  Pattern pattern,
                                  TimestampExtractor timestampExtractor,
                                  IDeserializer<object> keyDeserializer,
                                  IDeserializer<object> valDeserializer)
            : base(name, NO_PREDECESSORS)
        {
            this.topics = topics != null ? Arrays.asList(topics) : new List<>();
            this.pattern = pattern;
            this.keyDeserializer = keyDeserializer;
            this.valDeserializer = valDeserializer;
            this.timestampExtractor = timestampExtractor;
        }

        List<string> getTopics(Collection<string> subscribedTopics)
        {
            // if it is subscribed via patterns, it is possible that the topic metadata has not been updated
            // yet and hence the map from source node to topics is stale, in this case we put the pattern as a place holder;
            // this should only happen for debugging since during runtime this function should always be called after the metadata has updated.
            if (subscribedTopics.isEmpty())
            {
                return Collections.singletonList(string.valueOf(pattern));
            }

            List<string> matchedTopics = new List<>();
            foreach (string update in subscribedTopics)
            {
                if (pattern == topicToPatterns[update])
                {
                    matchedTopics.Add(update);
                }
                else if (topicToPatterns.ContainsKey(update) && isMatch(update))
                {
                    // the same topic cannot be matched to more than one pattern
                    // TODO: we should lift this requirement in the future
                    throw new TopologyException("Topic " + update +
                        " is already matched for another regex pattern " + topicToPatterns[update] +
                        " and hence cannot be matched to this regex pattern " + pattern + " any more.");
                }
                else if (isMatch(update))
                {
                    topicToPatterns.Add(update, pattern);
                    matchedTopics.Add(update);
                }
            }
            return matchedTopics;
        }


        public ProcessorNode build()
        {
            List<string> sourceTopics = nodeToSourceTopics[name];

            // if it is subscribed via patterns, it is possible that the topic metadata has not been updated
            // yet and hence the map from source node to topics is stale, in this case we put the pattern as a place holder;
            // this should only happen for debugging since during runtime this function should always be called after the metadata has updated.
            if (sourceTopics == null)
            {
                return new SourceNode<>(name, Collections.singletonList(string.valueOf(pattern)), timestampExtractor, keyDeserializer, valDeserializer);
            }
            else
            {

                return new SourceNode<>(name, maybeDecorateInternalSourceTopics(sourceTopics), timestampExtractor, keyDeserializer, valDeserializer);
            }
        }

        private bool isMatch(string topic)
        {
            return pattern.matcher(topic).matches();
        }


        Source describe()
        {
            return new Source(name, topics.size() == 0 ? null : new HashSet<>(topics), pattern);
        }
    }
}
