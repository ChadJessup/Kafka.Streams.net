using Confluent.Kafka;
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

        private const int VERSION = 0;

        private readonly FileInfo file;
        private readonly object lockObject;

        public OffsetCheckpoint(FileInfo file)
        {
            this.file = file;
            lockObject = new object();
        }

        /**
         * @throws IOException if any file operation fails with an IO exception
         */
        public void Write(Dictionary<TopicPartition, long?> offsets)
        {
            // if there is no offsets, skip writing the file to save disk IOs
            if (offsets == null || !offsets.Any())
            {
                return;
            }

            lock (lockObject)
            {
                // write to temp file and then swap with the existing file
                FileStream temp = File.OpenWrite(file.FullName + ".tmp");
                LOG.LogTrace("Writing tmp checkpoint file {}", temp.Name);

                var fileOutputStream = new FileStream(temp.SafeFileHandle, FileAccess.Write);
                using var writer = new StreamWriter(fileOutputStream, System.Text.Encoding.UTF8);

                WriteIntLine(writer, VERSION);
                WriteIntLine(writer, offsets.Count);

                foreach (var entry in offsets)
                {
                    WriteEntry(writer, entry.Key, entry.Value);
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
        private void WriteIntLine(StreamWriter writer, int number)
        {
            writer.Write(number.ToString());
            writer.WriteLine();
        }

        /**
         * @throws IOException if file write operations failed with any IO exception
         */
        private void WriteEntry(
            StreamWriter writer,
            TopicPartition part,
            long? offset)
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
        public Dictionary<TopicPartition, long> Read()
        {
            lock (lockObject)
            {
                try
                {
                    using var reader = new StreamReader(file.FullName);

                    var version = ReadInt(reader);
                    switch (version)
                    {
                        case 0:
                            var expectedSize = ReadInt(reader);
                            var offsets = new Dictionary<TopicPartition, long>();
                            var line = reader.ReadLine();

                            while (line != null)
                            {
                                var pieces = WHITESPACE_MINIMUM_ONCE.Split(line);
                                if (pieces.Length != 3)
                                {
                                    throw new IOException($"Malformed line in offset checkpoint file: '{line}'.");
                                }

                                var topic = pieces[0];
                                var partition = int.Parse(pieces[1]);
                                var offset = long.Parse(pieces[2]);
                                offsets.Add(new TopicPartition(topic, partition), offset);
                                line = reader.ReadLine();
                            }

                            if (offsets.Count != expectedSize)
                            {
                                throw new IOException($"Expected {expectedSize} entries but found only {offsets.Count}");
                            }

                            return offsets;

                        default:
                            throw new ArgumentException("Unknown offset checkpoint version: " + version);
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
        private int ReadInt(StreamReader reader)
        {
            var line = reader.ReadLine();
            if (line == null)
            {
                throw new Exception("FileInfo ended prematurely.");
            }

            return int.Parse(line);
        }

        /**
         * @throws IOException if there is any IO exception during delete
         */
        public void Delete()
        {
            file.Delete();
        }

        public override string ToString()
        {
            return file.FullName;
        }
    }
}