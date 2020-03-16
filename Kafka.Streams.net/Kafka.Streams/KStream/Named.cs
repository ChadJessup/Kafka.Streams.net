using Kafka.Streams.Errors;
using Kafka.Streams.KStream.Interfaces;
using System;

namespace Kafka.Streams.KStream
{
    public class Named : INamedOperation<Named>
    {
        private const int MAX_NAME_LENGTH = 249;

        public string? Name { get; }

        protected Named(Named named)
            : this(named?.Name)
        {
        }

        protected Named(string? name)
        {
            this.Name = name;
            if (name != null)
            {
                Validate(name);
            }
        }

        /**
         * Create a Named instance with provided name.
         *
         * @param name  the processor name to be used. If {@code null} a default processor name will be generated.
         * @return      A new {@link Named} instance configured with name
         *
         * @throws TopologyException if an invalid name is specified; valid characters are ASCII alphanumerics, '.', '_' and '-'.
         */
        public static Named As(string name)
        {
            if (name is null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            return new Named(name);
        }

        public virtual Named WithName(string name)
        {
            return new Named(name);
        }

        public static void Validate(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new TopologyException("Name is illegal, it can't be empty");
            }

            if (name.Equals(".") || name.Equals(".."))
            {
                throw new TopologyException("Name cannot be \".\" or \"..\"");
            }

            if (name.Length > MAX_NAME_LENGTH)
            {
                throw new TopologyException("Name is illegal, it can't be longer than " + MAX_NAME_LENGTH +
                        " characters, name: " + name);
            }

            if (!ContainsValidPattern(name))
                throw new TopologyException("Name \"" + name + "\" is illegal, it contains a character other than " +
                        "ASCII alphanumerics, '.', '_' and '-'");
        }

        /**
         * Valid characters for Kafka topics are the ASCII alphanumerics, '.', '_', and '-'
         */
        private static bool ContainsValidPattern(string topic)
        {
            for (var i = 0; i < topic.Length; ++i)
            {
                var c = topic[i];

                // We don't use Character.isLetterOrDigit(c) because it's slower
                var validLetterOrDigit = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z');
                var validChar = validLetterOrDigit || c == '.' || c == '_' || c == '-';
                if (!validChar)
                {
                    return false;
                }
            }

            return true;
        }
    }
}
