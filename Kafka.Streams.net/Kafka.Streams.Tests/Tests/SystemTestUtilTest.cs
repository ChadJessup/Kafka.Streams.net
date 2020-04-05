using Kafka.Streams.Tests.Tools;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Tests
{
    public class SystemTestUtilTest
    {

        private Dictionary<string, string> expectedParsedMap = new Dictionary<string, string>();

        public void SetUp()
        {
            expectedParsedMap.Add("foo", "foo1");
            expectedParsedMap.Add("bar", "bar1");
            expectedParsedMap.Add("baz", "baz1");
        }

        [Xunit.Fact]
        public void ShouldParseCorrectMap()
        {
            string formattedConfigs = "foo=foo1,bar=bar1,baz=baz1";
            Dictionary<string, string> parsedMap = SystemTestUtil.ParseConfigs(formattedConfigs);
            Dictionary<string, string> sortedParsedMap = new Dictionary<string, string>(parsedMap);
            Assert.Equal(sortedParsedMap, expectedParsedMap);
        }

        [Xunit.Fact]// // (expected = NullPointerException)
        public void ShouldThrowExceptionOnNull()
        {
            SystemTestUtil.ParseConfigs(null);
        }

        [Xunit.Fact]// // (expected = IllegalStateException)
        public void ShouldThrowExceptionIfNotCorrectKeyValueSeparator()
        {
            string badString = "foo:bar,baz:boo";
            SystemTestUtil.ParseConfigs(badString);
        }

        [Xunit.Fact]// // (expected = IllegalStateException)
        public void ShouldThrowExceptionIfNotCorrectKeyValuePairSeparator()
        {
            string badString = "foo=bar;baz=boo";
            SystemTestUtil.ParseConfigs(badString);
        }

        [Xunit.Fact]
        public void ShouldParseSingleKeyValuePairString()
        {
            Dictionary<string, string> expectedSinglePairMap = new Dictionary<string, string>();
            expectedSinglePairMap.Add("foo", "bar");
            string singleValueString = "foo=bar";
            Dictionary<string, string> parsedMap = SystemTestUtil.ParseConfigs(singleValueString);
            Assert.Equal(expectedSinglePairMap, parsedMap);
        }
    }
}
