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
using System.Collections.Generic;
/**
 * Used to capture subscribed topic via Patterns discovered during the
 * partition assignment process.
 */
public class SubscriptionUpdates
        {
            private HashSet<string> updatedTopicSubscriptions = new HashSet<>();

            private void updateTopics(List<string> topicNames)
            {
                updatedTopicSubscriptions.clear();
                updatedTopicSubscriptions.AddAll(topicNames);
            }

            public List<string> getUpdates()
            {
                return Collections.unmodifiableSet(updatedTopicSubscriptions);
            }

            bool hasUpdates()
            {
                return !updatedTopicSubscriptions.isEmpty();
            }


            public string ToString()
            {
                return string.Format("SubscriptionUpdates{updatedTopicSubscriptions=%s}", updatedTopicSubscriptions);
            }
        }
