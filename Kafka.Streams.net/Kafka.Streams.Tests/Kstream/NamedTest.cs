using Kafka.Streams.Errors;
using Kafka.Streams.KStream;
using System;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class NamedTest
    {
#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.

        [Fact]
        public void ShouldThrowExceptionGivenNullName()
        {
            Assert.Throws<ArgumentNullException>(() => Named.As(null));
        }

#pragma warning restore CS8625 // Cannot convert null literal to non-nullable reference type.

        [Fact]
        public void ShouldThrowExceptionOnInvalidTopicNames()
        {
            var longString = new char[250];
            Array.Fill(longString, 'a');
            string[] invalidNames = { "", "foo bar", "..", "foo:bar", "foo=bar", ".", new string(longString) };

            foreach (var name in invalidNames)
            {
                Assert.Throws<TopologyException>(() => Named.Validate(name));
            }
        }
    }
}
