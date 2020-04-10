using Kafka.Streams.KStream.Interfaces;

namespace Kafka.Streams.KStream.Internals
{
    public class NamedInternal : Named
    {
        public static NamedInternal Empty()
        {
            return new NamedInternal((string?)null);
        }

        public static NamedInternal With(string Name)
        {
            return new NamedInternal(Name);
        }

        /**
         * Creates a new {@link NamedInternal} instance.
         *
         * @param internal  the internal Name.
         */
        public NamedInternal(Named internalName)
            : base(internalName)
        {
        }

        /**
         * Creates a new {@link NamedInternal} instance.
         *
         * @param internal the internal Name.
         */
        public NamedInternal(string? internalName)
            : base(internalName)
        {
        }

        public override Named WithName(string Name)
        {
            return new NamedInternal(Name);
        }

        public string SuffixWithOrElseGet(string suffix, string other)
        {
            if (this.Name != null)
            {
                return this.Name + suffix;
            }
            else
            {
                return other;
            }
        }

        public string SuffixWithOrElseGet(string suffix, IInternalNameProvider provider, string prefix)
        {
            // We actually do not need to generate processor names for operation if a Name is specified.
            // But before returning, we still need to burn index for the operation to keep topology backward compatibility.
            if (this.Name != null)
            {
                provider.NewProcessorName(prefix);

                var suffixed = this.Name + suffix;
                // Re-validate generated Name as suffixed string could be too large.
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
            // We actually do not need to generate processor names for operation if a Name is specified.
            // But before returning, we still need to burn index for the operation to keep topology backward compatibility.
            if (this.Name != null)
            {
                provider.NewProcessorName(prefix);
                return this.Name;
            }
            else
            {
                return provider.NewProcessorName(prefix);
            }
        }
    }
}
