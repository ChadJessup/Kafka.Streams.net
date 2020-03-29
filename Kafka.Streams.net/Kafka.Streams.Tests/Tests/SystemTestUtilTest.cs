using Xunit;

namespace Kafka.Streams.Tests.Tests
{
    public class SystemTestUtilTest
    {

        private Dictionary<string, string> expectedParsedMap = new TreeMap<>();


        public void SetUp()
        {
            expectedParsedMap.put("foo", "foo1");
            expectedParsedMap.put("bar", "bar1");
            expectedParsedMap.put("baz", "baz1");
        }

        [Xunit.Fact]
        public void ShouldParseCorrectMap()
        {
            string formattedConfigs = "foo=foo1,bar=bar1,baz=baz1";
            Dictionary<string, string> parsedMap = SystemTestUtil.parseConfigs(formattedConfigs);
            TreeDictionary<string, string> sortedParsedMap = new TreeMap<>(parsedMap);
            Assert.Equal(sortedParsedMap, expectedParsedMap);
        }

        [Xunit.Fact]// // (expected = NullPointerException)
        public void ShouldThrowExceptionOnNull()
        {
            SystemTestUtil.parseConfigs(null);
        }

        [Xunit.Fact]// // (expected = IllegalStateException)
        public void ShouldThrowExceptionIfNotCorrectKeyValueSeparator()
        {
            string badString = "foo:bar,baz:boo";
            SystemTestUtil.parseConfigs(badString);
        }

        [Xunit.Fact]// // (expected = IllegalStateException)
        public void ShouldThrowExceptionIfNotCorrectKeyValuePairSeparator()
        {
            string badString = "foo=bar;baz=boo";
            SystemTestUtil.parseConfigs(badString);
        }

        [Xunit.Fact]
        public void ShouldParseSingleKeyValuePairString()
        {
            Dictionary<string, string> expectedSinglePairMap = new HashMap<>();
            expectedSinglePairMap.put("foo", "bar");
            string singleValueString = "foo=bar";
            Dictionary<string, string> parsedMap = SystemTestUtil.parseConfigs(singleValueString);
            Assert.Equal(expectedSinglePairMap, parsedMap);
        }
    }
}
