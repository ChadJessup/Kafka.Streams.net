using Confluent.Kafka;
using Kafka.Streams.Temp;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace Kafka.Streams.State.Internals
{
    /**
     * This saves out a map of topic/partition=&gt;offsets to a file. The string.Format of the file is UTF-8 text containing the following:
     * <pre>
     *   <version>
     *   <n>
     *   <topic_name_1> <partition_1> <offset_1>
     *   .
     *   .
     *   .
     *   <topic_name_n> <partition_n> <offset_n>
     * </pre>
     *   The first line contains a number designating the string.Format version (currently 0), the get line contains
     *   a number giving the total number of offsets. Each successive line gives a topic/partition/offset triple
     *   separated by spaces.
     */
    public class OffsetCheckpoint
    {
        private static readonly ILogger LOG = new LoggerFactory().CreateLogger<OffsetCheckpoint>();

        private static readonly Regex WHITESPACE_MINIMUM_ONCE = new Regex("\\s+", RegexOptions.Compiled);

        private static readonly int VERSION = 0;

        private readonly FileInfo file;
        private readonly object @lock;

        public OffsetCheckpoint(FileInfo file)
        {
            this.file = file;
            @lock = new object();
        }

        /**
         * @throws IOException if any file operation fails with an IO exception
         */
        public void write(Dictionary<TopicPartition, long> offsets)
        {
            // if there is no offsets, skip writing the file to save disk IOs
            if (!offsets.Any())
            {
                return;
            }

            lock (@lock)
            {
                // write to temp file and then swap with the existing file
                FileStream temp = File.OpenWrite(file.FullName + ".tmp");
                LOG.LogTrace("Writing tmp checkpoint file {}", temp.Name);

                FileStream fileOutputStream = new FileStream(temp.SafeFileHandle, FileAccess.Write);
                using var writer = new StreamWriter(fileOutputStream, System.Text.Encoding.UTF8);

                writeIntLine(writer, VERSION);
                writeIntLine(writer, offsets.Count);

                foreach (var entry in offsets)
                {
                    writeEntry(writer, entry.Key, entry.Value);
                }

                writer.Flush();
                //fileOutputStream.getFD().sync();

                LOG.LogTrace("Swapping tmp checkpoint file {} {}", temp.Name, file.FullName);
                File.Move(temp.Name, file.FullName);
            }
        }

        /**
         * @throws IOException if file write operations failed with any IO exception
         */
        private void writeIntLine(StreamWriter writer, int number)
        {
            writer.Write(number.ToString());
            writer.WriteLine();
        }

        /**
         * @throws IOException if file write operations failed with any IO exception
         */
        private void writeEntry(
            StreamWriter writer,
            TopicPartition part,
            long offset)
        {
            writer.Write(part.Topic);
            writer.Write(' ');
            writer.Write(part.Partition.ToString());
            writer.Write(' ');
            writer.Write(offset.ToString());
            writer.WriteLine();
        }


        /**
         * @throws IOException if any file operation fails with an IO exception
         * @throws ArgumentException if the offset checkpoint version is unknown
         */
        public Dictionary<TopicPartition, long> read()
        {
            lock (@lock)
            {
                try
                {
                    using BufferedReader reader = Files.newBufferedReader(file.FullName);

                    int version = readInt(reader);
                    switch (version)
                    {
                        case 0:
                            int expectedSize = readInt(reader);
                            Dictionary<TopicPartition, long> offsets = new Dictionary<TopicPartition, long>();
                            string line = reader.readLine();
                            while (line != null)
                            {
                                string[] pieces = WHITESPACE_MINIMUM_ONCE.Split(line);
                                if (pieces.Length != 3)
                                {
                                    throw new IOException(
                                        string.Format("Malformed line in offset checkpoint file: '%s'.", line));
                                }

                                string topic = pieces[0];
                                int partition = int.Parse(pieces[1]);
                                long offset = long.Parse(pieces[2]);
                                offsets.Add(new TopicPartition(topic, partition), offset);
                                line = reader.readLine();
                            }
                            if (offsets.Count != expectedSize)
                            {
                                throw new IOException(
                                    string.Format("Expected %d entries but found only %d", expectedSize, offsets.Count));
                            }

                            return offsets;

                        default:
                            throw new System.ArgumentException("Unknown offset checkpoint version: " + version);
                    }
                }
                catch (FileNotFoundException e)
                {
                    return new Dictionary<TopicPartition, long>();
                }
            }
        }

        /**
         * @throws IOException if file read ended prematurely
         */
        private int readInt(BufferedReader reader)
        {
            string line = reader.readLine();
            if (line == null)
            {
                throw new Exception("FileInfo ended prematurely.");
            }
            return int.Parse(line);
        }

        /**
         * @throws IOException if there is any IO exception during delete
         */
        public void delete()
        {
            file.Delete();
        }

        public override string ToString()
        {
            return file.FullName;
        }
    }
}