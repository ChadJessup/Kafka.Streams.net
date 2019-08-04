/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.streams.kstream.internals;

import java.util.Objects;

public class Change<T> {

    public  T newValue;
    public  T oldValue;

    public Change( T newValue,  T oldValue)
{
        this.newValue = newValue;
        this.oldValue = oldValue;
    }

    
    public string ToString()
{
        return "(" + newValue + "<-" + oldValue + ")";
    }

    
    public bool Equals( Object o)
{
        if (this == o)
{
            return true;
        }
        if (o == null || getClass() != o.getClass())
{
            return false;
        }
         Change<?> change = (Change<?>) o;
        return Objects.Equals(newValue, change.newValue) &&
                Objects.Equals(oldValue, change.oldValue);
    }

    
    public int hashCode()
{
        return Objects.hash(newValue, oldValue);
    }
}
