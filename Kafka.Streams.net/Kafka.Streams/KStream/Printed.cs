using Kafka.Streams.kstream;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using System;
using System.IO;
using System.Linq;

namespace Kafka.Streams.KStream
{
    /**
     * An object to define the options used when printing a {@link KStream}.
     *
     * @param key type
     * @param value type
     * @see KStream#print(Printed)
     */
    public class Printed<K, V> : INamedOperation<Printed<K, V>>
    {
        protected Stream outputStream;
        protected string label;
        protected string processorName;
        protected IKeyValueMapper<K, V, string> mapper = null;// new IKeyValueMapper<K, V, string>();

        public string apply(K key, V value)
        {
            return string.Format("%s, %s", key, value);
        }

        private Printed(Stream outputStream)
        {
            this.outputStream = outputStream;
        }

        /**
         * Copy constructor.
         * @param printed   instance of {@link Printed} to copy
         */
        protected Printed(Printed<K, V> printed)
        {
            this.outputStream = printed.outputStream;
            this.label = printed.label;
            this.mapper = printed.mapper;
            this.processorName = printed.processorName;
        }

        /**
         * Print the records of a {@link KStream} to a file.
         *
         * @param filePath path of the file
         * @param      key type
         * @param      value type
         * @return a new Printed instance
         */
        public static Printed<K, V> toFile(string filePath)
        {
            filePath = filePath ?? throw new ArgumentNullException("filePath can't be null", nameof(filePath));

            if (!filePath.Trim().Any())
            {
                throw new TopologyException("filePath can't be an empty string");
            }

            try
            {
                return new Printed<K, V>(File.OpenWrite(filePath));
            }
            catch (IOException e)
            {
                throw new TopologyException("Unable to write stream to file at [" + filePath + "] " + e.ToString());
            }
        }

        /**
         * Print the records of a {@link KStream} to system out.
         *
         * @param key type
         * @param value type
         * @return a new Printed instance
         */
        public static Printed<K, V> toSysOut()
        {
            return new Printed<K, V>(Console.OpenStandardOutput());
        }

        /**
         * Print the records of a {@link KStream} with the provided label.
         *
         * @param label label to use
         * @return this
         */
        public Printed<K, V> withLabel(string label)
        {
            this.label = label ?? throw new ArgumentNullException("label can't be null", nameof(label));

            return this;
        }

        /**
         * Print the records of a {@link KStream} with the provided {@link KeyValueMapper}
         * The provided KeyValueMapper's mapped value type must be {@code string}.
         * <p>
         * The example below shows how to customize output data.
         * <pre>{@code
         *  KeyValueMapper<int, string, string> mapper = new KeyValueMapper<int, string, string>()
{
         *     public string apply(int key, string value)
{
         *         return string.Format("(%d, %s)", key, value);
         *     }
         * };
         * }</pre>
         *
         * Implementors will need to override {@code ToString()} for keys and values that are not of type {@link string},
         * {@link int} etc. to get meaningful information.
         *
         * @param mapper mapper to use
         * @return this
         */
        public Printed<K, V> withKeyValueMapper(IKeyValueMapper<K, V, string> mapper)
        {
            this.mapper = mapper ?? throw new ArgumentNullException("mapper can't be null", nameof(mapper));
            return this;
        }

        /**
         * Print the records of a {@link KStream} with provided processor name.
         *
         * @param processorName the processor name to be used. If {@code null} a default processor name will be generated
         ** @return this
         */

        public Printed<K, V> WithName(string processorName)
        {
            this.processorName = processorName;
            return this;
        }
    }
}