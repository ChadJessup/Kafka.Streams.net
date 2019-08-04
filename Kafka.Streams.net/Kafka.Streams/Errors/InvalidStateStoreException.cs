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
namespace Kafka.Streams.Errors {


/**
 * Indicates that there was a problem when trying to access a
 * {@link org.apache.kafka.streams.processor.IStateStore IStateStore}, i.e, the Store is no longer valid because it is
 * closed or doesn't exist any more due to a rebalance.
 * <p>
 * These exceptions may be transient, i.e., during a rebalance it won't be possible to query the stores as they are
 * being (re)-initialized. Once the rebalance has completed the stores will be available again. Hence, it is valid
 * to backoff and retry when handling this exception.
 */
public class InvalidStateStoreException : StreamsException {

    private  static long serialVersionUID = 1L;

    public InvalidStateStoreException( string message)
{
        super(message);
    }

    public InvalidStateStoreException( string message,  Throwable throwable)
{
        super(message, throwable);
    }

    public InvalidStateStoreException( Throwable throwable)
{
        super(throwable);
    }
}
