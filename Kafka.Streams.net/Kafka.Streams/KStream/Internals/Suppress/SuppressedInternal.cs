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
namespace Kafka.Streams.KStream.Internals.Suppress
{
public class SuppressedInternal<K> : ISuppressed<K>, INamedSuppressed<K>
{
    private static TimeSpan DEFAULT_SUPPRESSION_TIME = Duration.ofMillis(long.MaxValue);
    private static StrictBufferConfigImpl DEFAULT_BUFFER_CONFIG = (StrictBufferConfigImpl)BufferConfig.unbounded();

    private string name;
    private BufferConfigInternal bufferConfig;
    private TimeSpan timeToWaitForMoreEvents;
    private TimeDefinition<K> timeDefinition;
    private bool safeToDropTombstones;

    /**
     * @param safeToDropTombstones Note: it's *only* safe to drop tombstones for windowed KTables in " results" mode.
     *                             In that case, we have a priori knowledge that we have never before emitted any
     *                             results for a given key, and therefore the tombstone is unnecessary (albeit
     *                             idempotent and correct). We decided that the unnecessary tombstones would not be
     *                             desirable in the output stream, though, hence the ability to drop them.
     *
     *                             A alternative is to remember whether a result has previously been emitted
     *                             for a key and drop tombstones in that case, but it would be a little complicated to
     *                             figure out when to forget the fact that we have emitted some result (currently, the
     *                             buffer immediately forgets all about a key when we emit, which helps to keep it
     *                             compact).
     */
    public SuppressedInternal(string name,
                               TimeSpan suppressionTime,
                               BufferConfig bufferConfig,
                               TimeDefinition<K> timeDefinition,
                               bool safeToDropTombstones)
    {
        this.name = name;
        this.timeToWaitForMoreEvents = suppressionTime == null ? DEFAULT_SUPPRESSION_TIME : suppressionTime;
        this.timeDefinition = timeDefinition == null ? TimeDefinitions.RecordTimeDefintion.instance() : timeDefinition;
        this.bufferConfig = bufferConfig == null ? DEFAULT_BUFFER_CONFIG : (BufferConfigInternal)bufferConfig;
        this.safeToDropTombstones = safeToDropTombstones;
    }


    public ISuppressed<K> withName(string name)
    {
        return new SuppressedInternal<>(name, timeToWaitForMoreEvents, bufferConfig, timeDefinition, safeToDropTombstones);
    }


    public string name
    {
        return name;
    }

    BufferConfigInternal bufferConfig()
    {
        return bufferConfig;
    }

    TimeDefinition<K> timeDefinition()
    {
        return timeDefinition;
    }

    TimeSpan timeToWaitForMoreEvents()
    {
        return timeToWaitForMoreEvents == null ? Duration.ZERO : timeToWaitForMoreEvents;
    }

    bool safeToDropTombstones()
    {
        return safeToDropTombstones;
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
        SuppressedInternal <object> that = (SuppressedInternal <object>) o;
        return safeToDropTombstones == that.safeToDropTombstones &&
            Objects.Equals(name, that.name) &&
            Objects.Equals(bufferConfig, that.bufferConfig) &&
            Objects.Equals(timeToWaitForMoreEvents, that.timeToWaitForMoreEvents) &&
            Objects.Equals(timeDefinition, that.timeDefinition);
    }


    public int hashCode()
    {
        return Objects.hash(name, bufferConfig, timeToWaitForMoreEvents, timeDefinition, safeToDropTombstones);
    }


    public string ToString()
    {
        return "SuppressedInternal{" +
                "name='" + name + '\'' +
                ", bufferConfig=" + bufferConfig +
                ", timeToWaitForMoreEvents=" + timeToWaitForMoreEvents +
                ", timeDefinition=" + timeDefinition +
                ", safeToDropTombstones=" + safeToDropTombstones +
                '}';
    }
}
