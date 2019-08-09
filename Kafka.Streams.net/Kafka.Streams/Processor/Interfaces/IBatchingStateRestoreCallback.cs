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

namespace Kafka.Streams.Processor
{
    /**
     * Interface for batching restoration of a {@link IStateStore}
     *
     * It is expected that implementations of this will not call the {@link StateRestoreCallback#restore(byte[],
     * byte[]]} method.
     */
    public interface IBatchingStateRestoreCallback : IStateRestoreCallback
    {
        /**
         * Called to restore a number of records.  This method is called repeatedly until the {@link IStateStore} is fulled
         * restored.
         *
         * @param records the records to restore.
         */
        void restoreAll(List<KeyValue<byte[], byte[]>> records);

    }
}