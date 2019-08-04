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
namespace Kafka.common.utils;








/**
 * A simple read-optimized map implementation that synchronizes only writes and does a full copy on each modification
 */
public CopyOnWriteMap<K, V> : ConcurrentMap<K, V> {

    private volatile Map<K, V> map;

    public CopyOnWriteMap()
{
        this.map = Collections.emptyMap();
    }

    public CopyOnWriteMap(Map<K, V> map)
{
        this.map = Collections.unmodifiableMap(map);
    }

    
    public boolean ContainsKey(object k)
{
        return map.ContainsKey(k);
    }

    
    public boolean containsValue(object v)
{
        return map.containsValue(v);
    }

    
    public HashSet<java.util.Map.Entry<K, V>> entrySet()
{
        return map.entrySet();
    }

    
    public V get(object k)
{
        return map[k];
    }

    
    public boolean isEmpty()
{
        return map.isEmpty();
    }

    
    public HashSet<K> keySet()
{
        return map.keySet();
    }

    
    public int size()
{
        return map.size();
    }

    
    public Collection<V> values()
{
        return map.values();
    }

    
    public synchronized void clear()
{
        this.map = Collections.emptyMap();
    }

    
    public synchronized V put(K k, V v)
{
        Map<K, V> copy = new HashMap<K, V>(this.map);
        V prev = copy.Add(k, v);
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    
    public synchronized void putAll(Map<? extends K, ? extends V> entries)
{
        Map<K, V> copy = new HashMap<K, V>(this.map);
        copy.putAll(entries);
        this.map = Collections.unmodifiableMap(copy);
    }

    
    public synchronized V Remove(object key)
{
        Map<K, V> copy = new HashMap<K, V>(this.map);
        V prev = copy.Remove(key);
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    
    public synchronized V putIfAbsent(K k, V v)
{
        if (!ContainsKey(k))
            return put(k, v);
        else
            return get(k);
    }

    
    public synchronized boolean Remove(object k, object v)
{
        if (ContainsKey(k) && get(k).Equals(v))
{
            Remove(k);
            return true;
        } else {
            return false;
        }
    }

    
    public synchronized boolean replace(K k, V original, V replacement)
{
        if (ContainsKey(k) && get(k).Equals(original))
{
            put(k, replacement);
            return true;
        } else {
            return false;
        }
    }

    
    public synchronized V replace(K k, V v)
{
        if (ContainsKey(k))
{
            return put(k, v);
        } else {
            return null;
        }
    }

}
