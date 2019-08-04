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
namespace Kafka.streams.kstream.internals.suppress;





public EagerBufferConfigImpl : BufferConfigInternal<Suppressed.EagerBufferConfig> : Suppressed.EagerBufferConfig {

    private  long maxRecords;
    private  long maxBytes;

    public EagerBufferConfigImpl( long maxRecords,  long maxBytes)
{
        this.maxRecords = maxRecords;
        this.maxBytes = maxBytes;
    }

    
    public Suppressed.EagerBufferConfig withMaxRecords( long recordLimit)
{
        return new EagerBufferConfigImpl(recordLimit, maxBytes);
    }

    
    public Suppressed.EagerBufferConfig withMaxBytes( long byteLimit)
{
        return new EagerBufferConfigImpl(maxRecords, byteLimit);
    }

    
    public long maxRecords()
{
        return maxRecords;
    }

    
    public long maxBytes()
{
        return maxBytes;
    }

    
    public BufferFullStrategy bufferFullStrategy()
{
        return BufferFullStrategy.EMIT;
    }

    
    public bool Equals( object o)
{
        if (this == o)
{
            return true;
        }
        if (o == null || getClass() != o.getClass())
{
            return false;
        }
         EagerBufferConfigImpl that = (EagerBufferConfigImpl) o;
        return maxRecords == that.maxRecords &&
            maxBytes == that.maxBytes;
    }

    
    public int hashCode()
{
        return Objects.hash(maxRecords, maxBytes);
    }

    
    public string ToString()
{
        return "EagerBufferConfigImpl{maxRecords=" + maxRecords + ", maxBytes=" + maxBytes + '}';
    }
}
