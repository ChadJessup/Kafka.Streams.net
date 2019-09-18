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
using System;
using System.Runtime.Serialization;

namespace Kafka.Streams.Processor.Internals
{
    [Serializable]
    internal class TaskMigratedException<K, V> : Exception
    {
        private readonly StreamTask<K, V> streamTask;
        private readonly ProducerFencedException fatal;
        private readonly StreamTask<object, object> streamTask1;
        private readonly ProducerFencedException e;

        public TaskMigratedException()
        {
        }

        public TaskMigratedException(string message)
            : base(message)
        {
        }

        public TaskMigratedException(StreamTask<K, V> streamTask, ProducerFencedException fatal)
        {
            this.streamTask = streamTask;
            this.fatal = fatal;
        }

        public TaskMigratedException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public TaskMigratedException(StreamTask<object, object> streamTask1, ProducerFencedException e)
        {
            this.streamTask1 = streamTask1;
            this.e = e;
        }

        protected TaskMigratedException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}