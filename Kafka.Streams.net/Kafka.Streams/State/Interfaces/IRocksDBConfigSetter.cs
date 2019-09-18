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
using RocksDbSharp;
using System.Collections.Generic;

namespace Kafka.Streams.State.Interfaces
{
    /**
     * An interface to that allows developers to customize the RocksDb settings for a given Store.
     * Please read the <a href="https://github.com/facebook/rocksdb/wiki/RocksDb-Tuning-Guide">RocksDb Tuning Guide</a>.
     *
     * Note: if you choose to modify the {@code org.rocksdb.BlockBasedTableConfig} you should retrieve a reference to
     * the existing one (rather than create a new BlockBasedTableConfig object) so as to not lose the other default settings.
     * This can be done as {@code BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();}
     */
    public interface IRocksDbConfigSetter
    {
        //ILogger LOG = new LoggerFactory().CreateLogger<RocksDbConfigSetter>();

        /**
         * Set the rocks db options for the provided storeName.
         *
         * @param storeName     the name of the store being configured
         * @param options       the RocksDb options
         * @param configs       the configuration supplied to {@link org.apache.kafka.streams.StreamsConfig}
         */
        void setConfig(string storeName, DbOptions options, Dictionary<string, object> configs);

        /**
         * Close any user-constructed objects that inherit from {@code org.rocksdb.RocksObject}.
         * <p>
         * Any object created with {@code new} in {@link RocksDbConfigSetter#setConfig setConfig()} and that inherits
         * from {@code org.rocksdb.RocksObject} should have {@code org.rocksdb.RocksObject#close()}
         * called on it here to avoid leaking off-heap memory. Objects to be closed can be saved by the user or retrieved
         * back from {@code options} using its getter methods.
         * <p>
         * Example objects needing to be closed include {@code org.rocksdb.Filter} and {@code org.rocksdb.Cache}.
         *
         * @param storeName     the name of the store being configured
         * @param options       the RocksDb options
         */
        void close(string storeName, DbOptions options);
        //{
        //    LOG.LogWarning("The default close will be removed in 3.0.0 -- you should overwrite it if you have implemented RocksDbConfigSetter");
        //}
    }
}