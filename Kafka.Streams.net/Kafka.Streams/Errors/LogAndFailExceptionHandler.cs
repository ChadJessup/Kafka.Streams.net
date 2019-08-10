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
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.IProcessor.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using static Kafka.Streams.Errors.Interfaces.IDeserializationExceptionHandler;

namespace Kafka.Streams.Errors
{
    /**
     * Deserialization handler that logs a deserialization exception and then
     * signals the processing pipeline to stop processing more records and fail.
     */
    public class LogAndFailExceptionHandler : IDeserializationExceptionHandler
    {

        private static ILogger log = new LoggerFactory().CreateLogger<LogAndFailExceptionHandler>();


        public DeserializationHandlerResponse handle(IProcessorContext<K, V> context,
                                                      ConsumeResult<byte[], byte[]> record,
                                                      Exception exception)
        {

            log.LogError("Exception caught during Deserialization, " +
                      "taskId: {}, topic: {}, partition: {}, offset: {}",
                      context.taskId(), record.Topic, record.partition(), record.offset(),
                      exception);

            return DeserializationHandlerResponse.FAIL;
        }


        public void configure(Dictionary<string, object> configs)
        {
            // ignore
        }
    }
}