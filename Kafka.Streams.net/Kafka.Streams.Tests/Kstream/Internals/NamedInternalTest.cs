using Kafka.Streams.KStream.Interfaces;
using Xunit;

namespace Kafka.Streams.KStream.Internals
{
    public class NamedInternalTest
    {
        private const string TEST_PREFIX = "prefix-";
        private const string TEST_VALUE = "default-value";
        private const string TEST_SUFFIX = "-suffix";

        private class TestNameProvider : IInternalNameProvider
        {
            int index = 0;

            public string NewProcessorName(string prefix)
            {
                return prefix + "PROCESSOR-" + this.index++;
            }

            public string NewStoreName(string prefix)
            {
                return prefix + "STORE-" + this.index++;
            }
        }

        [Fact]
        public void ShouldSuffixNameOrReturnProviderValue()
        {
            var Name = "foo";
            var provider = new TestNameProvider();

            Assert.Equal(
                 Name + TEST_SUFFIX,
                 NamedInternal.With(Name).SuffixWithOrElseGet(TEST_SUFFIX, provider, TEST_PREFIX)
             );

            //1, not 0, indicates that the named call still burned an index number.
            Assert.Equal(
                 "prefix-PROCESSOR-1",
                 NamedInternal.With(null).SuffixWithOrElseGet(TEST_SUFFIX, provider, TEST_PREFIX)
             );
        }

        [Fact]
        public void ShouldGenerateWithPrefixGivenEmptyName()
        {
            var prefix = "KSTREAM-MAP-";
            Assert.Equal(prefix + "PROCESSOR-0", NamedInternal.With(null).OrElseGenerateWithPrefix(
                 new TestNameProvider(),
                 prefix)
             );
        }

        [Fact]
        public void ShouldNotGenerateWithPrefixGivenValidName()
        {
            var validName = "validName";
            Assert.Equal(validName, NamedInternal.With(validName).OrElseGenerateWithPrefix(new TestNameProvider(), "KSTREAM-MAP-")
             );
        }
    }
}
