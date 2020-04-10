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

        protected Named(string? Name)
        {
            this.Name = Name;
            if (Name != null)
            {
                Validate(Name);
            }
        }

        /**
         * Create a Named instance with provided Name.
         *
         * @param Name  the processor Name to be used. If {@code null} a default processor Name will be generated.
         * @return      A new {@link Named} instance configured with Name
         *
         * @throws TopologyException if an invalid Name is specified; valid characters are ASCII alphanumerics, '.', '_' and '-'.
         */
        public static Named As(string Name)
        {
            if (Name is null)
            {
                throw new ArgumentNullException(nameof(Name));
            }

            return new Named(Name);
        }

        public virtual Named WithName(string Name)
        {
            return new Named(Name);
        }

        public static void Validate(string Name)
        {
            if (Name == null)
            {
                throw new ArgumentNullException(nameof(Name));
            }

            if (Name.Length == 0 || Name.Equals(".") || Name.Equals(".."))
            {
                throw new ArgumentException("Name cannot be \"\", \".\", or \"..\"");
            }

            if (Name.Length > MAX_NAME_LENGTH)
            {
                throw new ArgumentException("Name is illegal, it can't be longer than " + MAX_NAME_LENGTH +
                        " characters, Name: " + Name);
            }

            if (!ContainsValidPattern(Name))
            {
                throw new ArgumentException("Name \"" + Name + "\" is illegal, it contains a character other than " +
                        "ASCII alphanumerics, '.', '_' and '-'");
            }
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
