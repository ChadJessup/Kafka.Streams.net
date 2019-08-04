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
namespace Kafka.streams.kstream.internals;



public class NamedInternal : Named {

    public static NamedInternal empty()
{
        return new NamedInternal((string) null);
    }

    public static NamedInternal with( string name)
{
        return new NamedInternal(name);
    }

    /**
     * Creates a new {@link NamedInternal} instance.
     *
     * @param internal  the internal name.
     */
    NamedInternal( Named internal)
{
        super(internal);
    }

    /**
     * Creates a new {@link NamedInternal} instance.
     *
     * @param internal the internal name.
     */
    NamedInternal( string internal)
{
        super(internal);
    }

    /**
     * @return a string name.
     */
    public string name()
{
        return name;
    }

    
    public NamedInternal withName( string name)
{
        return new NamedInternal(name);
    }
    
    string suffixWithOrElseGet( string suffix,  string other)
{
        if (name != null)
{
            return name + suffix;
        } else {
            return other;
        }
    }

    string suffixWithOrElseGet( string suffix,  InternalNameProvider provider,  string prefix)
{
        // We actually do not need to generate processor names for operation if a name is specified.
        // But before returning, we still need to burn index for the operation to keep topology backward compatibility.
        if (name != null)
{
            provider.newProcessorName(prefix);

             string suffixed = name + suffix;
            // Re-validate generated name as suffixed string could be too large.
            Named.validate(suffixed);

            return suffixed;
        } else {
            return provider.newProcessorName(prefix);
        }
    }

    string orElseGenerateWithPrefix( InternalNameProvider provider,  string prefix)
{
        // We actually do not need to generate processor names for operation if a name is specified.
        // But before returning, we still need to burn index for the operation to keep topology backward compatibility.
        if (name != null)
{
            provider.newProcessorName(prefix);
            return name;
        }  else {
            return provider.newProcessorName(prefix);
        }
    }

}
