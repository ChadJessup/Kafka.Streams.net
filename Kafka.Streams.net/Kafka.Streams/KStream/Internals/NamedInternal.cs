namespace Kafka.Streams.KStream.Internals
{
    public class NamedInternal : Named
    {

        public static NamedInternal empty()
        {
            return new NamedInternal((string)null);
        }

        public static NamedInternal with(string name)
        {
            return new NamedInternal(name);
        }

        /**
         * Creates a new {@link NamedInternal} instance.
         *
         * @param internal  the internal name.
         */
        public NamedInternal(Named @internal)
            : base(@internal)
        {
        }

        /**
         * Creates a new {@link NamedInternal} instance.
         *
         * @param internal the internal name.
         */
        public NamedInternal(string @internal)
            : base(@internal)
        {
        }

        public NamedInternal withName(string name)
        {
            return new NamedInternal(name);
        }

        public string suffixWithOrElseGet(string suffix, string other)
        {
            if (name != null)
            {
                return name + suffix;
            }
            else
            {
                return other;
            }
        }

        public string suffixWithOrElseGet(string suffix, InternalNameProvider provider, string prefix)
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
            }
            else
            {

                return provider.newProcessorName(prefix);
            }
        }

        public string orElseGenerateWithPrefix(InternalNameProvider provider, string prefix)
        {
            // We actually do not need to generate processor names for operation if a name is specified.
            // But before returning, we still need to burn index for the operation to keep topology backward compatibility.
            if (name != null)
            {
                provider.newProcessorName(prefix);
                return name;
            }
            else
            {
                return provider.newProcessorName(prefix);
            }
        }
    }
}