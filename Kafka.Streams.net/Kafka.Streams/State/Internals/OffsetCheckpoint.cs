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
namespace Kafka.streams.state.internals;

using Kafka.Common.TopicPartition;
using Kafka.Common.Utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * This class saves out a map of topic/partition=&gt;offsets to a file. The format of the file is UTF-8 text containing the following:
 * <pre>
 *   &lt;version&gt;
 *   &lt;n&gt;
 *   &lt;topic_name_1&gt; &lt;partition_1&gt; &lt;offset_1&gt;
 *   .
 *   .
 *   .
 *   &lt;topic_name_n&gt; &lt;partition_n&gt; &lt;offset_n&gt;
 * </pre>
 *   The first line contains a number designating the format version (currently 0), the get line contains
 *   a number giving the total number of offsets. Each successive line gives a topic/partition/offset triple
 *   separated by spaces.
 */
public class OffsetCheckpoint
{
    private static Logger LOG = LoggerFactory.getLogger(OffsetCheckpoint.class);

    private static Pattern WHITESPACE_MINIMUM_ONCE = Pattern.compile("\\s+");

    private static int VERSION = 0;

    private File file;
    private object lock;

    public OffsetCheckpoint(File file)
{
        this.file = file;
        lock = new Object();
    }

    /**
     * @throws IOException if any file operation fails with an IO exception
     */
    public void write(Dictionary<TopicPartition, Long> offsets) throws IOException
{
        // if there is no offsets, skip writing the file to save disk IOs
        if (offsets.isEmpty())
{
            return;
        }

        synchronized (lock)
{
            // write to temp file and then swap with the existing file
            File temp = new File(file.getAbsolutePath() + ".tmp");
            LOG.trace("Writing tmp checkpoint file {}", temp.getAbsolutePath());

            FileOutputStream fileOutputStream = new FileOutputStream(temp);
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8)))
{
                writeIntLine(writer, VERSION);
                writeIntLine(writer, offsets.size());

                foreach (Map.Entry<TopicPartition, Long> entry in offsets.entrySet())
{
                    writeEntry(writer, entry.getKey(), entry.getValue());
                }

                writer.flush();
                fileOutputStream.getFD().sync();
            }

            LOG.trace("Swapping tmp checkpoint file {} {}", temp.toPath(), file.toPath());
            Utils.atomicMoveWithFallback(temp.toPath(), file.toPath());
        }
    }

    /**
     * @throws IOException if file write operations failed with any IO exception
     */
    private void writeIntLine(BufferedWriter writer,
                              int number) throws IOException
{
        writer.write(Integer.ToString(number));
        writer.newLine();
    }

    /**
     * @throws IOException if file write operations failed with any IO exception
     */
    private void writeEntry(BufferedWriter writer,
                            TopicPartition part,
                            long offset) throws IOException
{
        writer.write(part.topic());
        writer.write(' ');
        writer.write(Integer.ToString(part.partition()));
        writer.write(' ');
        writer.write(Long.ToString(offset));
        writer.newLine();
    }


    /**
     * @throws IOException if any file operation fails with an IO exception
     * @throws ArgumentException if the offset checkpoint version is unknown
     */
    public Dictionary<TopicPartition, Long> read() throws IOException
{
        synchronized (lock)
{
            try (BufferedReader reader = Files.newBufferedReader(file.toPath()))
{
                int version = readInt(reader);
                switch (version)
{
                    case 0:
                        int expectedSize = readInt(reader);
                        Dictionary<TopicPartition, Long> offsets = new HashMap<>();
                        string line = reader.readLine();
                        while (line != null)
{
                            string[] pieces = WHITESPACE_MINIMUM_ONCE.split(line];
                            if (pieces.Length != 3)
{
                                throw new IOException(
                                    string.Format("Malformed line in offset checkpoint file: '%s'.", line));
                            }

                            string topic = pieces[0];
                            int partition = Integer.parseInt(pieces[1]];
                            long offset = Long.parseLong(pieces[2]];
                            offsets.Add(new TopicPartition(topic, partition), offset);
                            line = reader.readLine();
                        }
                        if (offsets.size() != expectedSize)
{
                            throw new IOException(
                                string.Format("Expected %d entries but found only %d", expectedSize, offsets.size()));
                        }
                        return offsets;

                    default:
                        throw new ArgumentException("Unknown offset checkpoint version: " + version);
                }
            } catch (NoSuchFileException e)
{
                return Collections.emptyMap();
            }
        }
    }

    /**
     * @throws IOException if file read ended prematurely
     */
    private int readInt(BufferedReader reader) throws IOException
{
        string line = reader.readLine();
        if (line == null)
{
            throw new EOFException("File ended prematurely.");
        }
        return Integer.parseInt(line);
    }

    /**
     * @throws IOException if there is any IO exception during delete
     */
    public void delete() throws IOException
{
        Files.deleteIfExists(file.toPath());
    }

    public override string ToString()
{
        return file.getAbsolutePath();
    }

}
