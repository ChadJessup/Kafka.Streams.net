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
namespace Kafka.Streams.Errors
{


    /**
     * Indicates a run time error incurred while trying to assign
     * {@link org.apache.kafka.streams.processor.Internals.StreamTask<K, V> stream tasks} to
     * {@link org.apache.kafka.streams.processor.Internals.StreamThread threads}.
     */
    public class TaskAssignmentException : StreamsException
    {


        private static readonly long serialVersionUID = 1L;

        public TaskAssignmentException(string message)
            : base(message)
        {
        }

        public TaskAssignmentException(string message, Throwable throwable)
            : base(message, throwable)
        {
        }

        public TaskAssignmentException(Throwable throwable)
            : base(throwable)
        {
        }
    }
}
