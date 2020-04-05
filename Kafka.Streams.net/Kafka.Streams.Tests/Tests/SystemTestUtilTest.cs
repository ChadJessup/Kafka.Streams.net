using Kafka.Streams.Tests.Tools;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Tests
{
    public class SystemTestUtilTest
    {
        private Dictionary<string, string> expectedParsedMap = new Dictionary<string, string>();

        public SystemTestUtilTest()
        {
            expectedParsedMap.Add("foo", "foo1");
            expectedParsedMap.Add("bar", "bar1");
            expectedParsedMap.Add("baz", "baz1");
        }

        [Fact]
        public void ShouldParseCorrectMap()
        {
            string formattedConfigs = "foo=foo1,bar=bar1,baz=baz1";
            Dictionary<string, string> parsedMap = SystemTestUtil.ParseConfigs(formattedConfigs);
            Dictionary<string, string> sortedParsedMap = new Dictionary<string, string>(parsedMap);
            Assert.Equal(sortedParsedMap, expectedParsedMap);
        }

        [Fact]
        public void ShouldThrowExceptionOnNull()
        {
            Assert.Throws<ArgumentNullException>(() => SystemTestUtil.ParseConfigs(null));
        }

        [Fact]
        public void ShouldThrowExceptionIfNotCorrectKeyValueSeparator()
        {
            string badString = "foo:bar,baz:boo";
            var e = Assert.Throws<ArgumentException>(() => SystemTestUtil.ParseConfigs(badString));

            Assert.Equal("Provided string [ foo:bar,baz:boo ] does not have expected key-value separator of '='", e.Message);
        }

        [Fact]
        public void ShouldThrowExceptionIfNotCorrectKeyValuePairSeparator()
        {
            string badString = "foo=bar;baz=boo";
            var e = Assert.Throws<ArgumentException>(() => SystemTestUtil.ParseConfigs(badString));
            Assert.Equal("Provided string [ foo=bar;baz=boo ] does not have expected key-value pair separator of ','", e.Message);
        }

        [Fact]
        public void ShouldParseSingleKeyValuePairString()
        {
            Dictionary<string, string> expectedSinglePairMap = new Dictionary<string, string>
            {
                { "foo", "bar" },
            };

            string singleValueString = "foo=bar";
            Dictionary<string, string> parsedMap = SystemTestUtil.ParseConfigs(singleValueString);
            Assert.Equal(expectedSinglePairMap, parsedMap);
        }
    }
}
