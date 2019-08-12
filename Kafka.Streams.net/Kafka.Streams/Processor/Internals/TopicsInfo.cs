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
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public class TopicsInfo
    {
        HashSet<string> sinkTopics;
        HashSet<string> sourceTopics;
        public Dictionary<string, InternalTopicConfig> stateChangelogTopics;
        public Dictionary<string, InternalTopicConfig> repartitionSourceTopics;

        public TopicsInfo(
            HashSet<string> sinkTopics,
            HashSet<string> sourceTopics,
            Dictionary<string, InternalTopicConfig> repartitionSourceTopics,
            Dictionary<string, InternalTopicConfig> stateChangelogTopics)
        {
            this.sinkTopics = sinkTopics;
            this.sourceTopics = sourceTopics;
            this.stateChangelogTopics = stateChangelogTopics;
            this.repartitionSourceTopics = repartitionSourceTopics;
        }

        public override bool Equals(object o)
        {
            if (o is TopicsInfo)
            {
                TopicsInfo other = (TopicsInfo)o;
                return other.sourceTopics.Equals(sourceTopics) && other.stateChangelogTopics.Equals(stateChangelogTopics);
            }
            else
            {

                return false;
            }
        }

        public override int GetHashCode()
        {
            long n = ((long)sourceTopics.GetHashCode() << 32) | (long)stateChangelogTopics.GetHashCode();
            return (int)(n % 0xFFFFFFFFL);
        }

        public override string ToString()
        {
            return "TopicsInfo{" +
                "sinkTopics=" + sinkTopics +
                ", sourceTopics=" + sourceTopics +
                ", repartitionSourceTopics=" + repartitionSourceTopics +
                ", stateChangelogTopics=" + stateChangelogTopics +
                '}';
        }
    }
}