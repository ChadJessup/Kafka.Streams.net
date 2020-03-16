using Kafka.Streams.KStream.Interfaces;

namespace Kafka.Streams.KStream.Internals
{
    public class NamedInternal : Named
    {
        public static NamedInternal Empty()
        {
            return new NamedInternal((string?)null);
        }

        public static NamedInternal With(string name)
        {
            return new NamedInternal(name);
        }

        /**
         * Creates a new {@link NamedInternal} instance.
         *
         * @param internal  the internal name.
         */
        public NamedInternal(Named internalName)
            : base(internalName)
        {
        }

        /**
         * Creates a new {@link NamedInternal} instance.
         *
         * @param internal the internal name.
         */
        public NamedInternal(string? internalName)
            : base(internalName)
        {
        }

        public override Named WithName(string name)
        {
            return new NamedInternal(name);
        }

        public string SuffixWithOrElseGet(string suffix, string other)
        {
            if (Name != null)
            {
                return Name + suffix;
            }
            else
            {
                return other;
            }
        }

        public string SuffixWithOrElseGet(string suffix, IInternalNameProvider provider, string prefix)
        {
            // We actually do not need to generate processor names for operation if a name is specified.
            // But before returning, we still need to burn index for the operation to keep topology backward compatibility.
            if (Name != null)
            {
                provider.NewProcessorName(prefix);

                var suffixed = Name + suffix;
                // Re-validate generated name as suffixed string could be too large.
                Named.Validate(suffixed);

                return suffixed;
            }
            else
            {

                return provider.NewProcessorName(prefix);
            }
        }

        public string OrElseGenerateWithPrefix(IInternalNameProvider provider, string prefix)
        {
            // We actually do not need to generate processor names for operation if a name is specified.
            // But before returning, we still need to burn index for the operation to keep topology backward compatibility.
            if (Name != null)
            {
                provider.NewProcessorName(prefix);
                return Name;
            }
            else
            {
                return provider.NewProcessorName(prefix);
            }
        }
    }
}
