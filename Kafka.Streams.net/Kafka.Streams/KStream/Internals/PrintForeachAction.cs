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
namespace Kafka.Streams.KStream.Internals {









public PrintForeachAction<K, V> : ForeachAction<K, V> {

    private  string label;
    private  PrintWriter printWriter;
    private  bool closable;
    private  IKeyValueMapper<K, V, string> mapper;

    /**
     * Print customized output with given writer. The {@link Stream} can be {@link System#out} or the others.
     *
     * @param outputStream The output stream to write to.
     * @param mapper The mapper which can allow user to customize output will be printed.
     * @param label The given name will be printed.
     */
    PrintForeachAction( Stream outputStream,
                        IKeyValueMapper<K, V, string> mapper,
                        string label)
{
        this.printWriter = new PrintWriter(new OutputStreamWriter(outputStream, System.Text.Encoding.UTF8));
        this.closable = outputStream != System.out && outputStream != System.err;
        this.mapper = mapper;
        this.label = label;
    }

    
    public void apply( K key,  V value)
{
         string data = string.Format("[%s]: %s", label, mapper.apply(key, value));
        printWriter.println(data);
        if (!closable)
{
            printWriter.flush();
        }
    }

    public void close()
{
        if (closable)
{
            printWriter.close();
        } else {
            printWriter.flush();
        }
    }

}
