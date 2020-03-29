using Confluent.Kafka;
using Xunit;

namespace Kafka.Streams.Tests.Tools
{
    /**
     * Class for common convenience methods for working on
     * System tests
     */

    public class SystemTestUtil {

        private static int KEY = 0;
        private static int VALUE = 1;

        /**
         * Takes a string with keys and values separated by '=' and each key value pair
         * separated by ',' for example max.block.ms=5000,retries=6,request.timeout.ms=6000
         *
         * This class makes it easier to pass configs from the system test in python to the Java test.
         *
         * @param formattedConfigs the formatted config string
         * @return HashMap with keys and values inserted
         */
        public static Dictionary<string, string> parseConfigs(string formattedConfigs) {
            Objects.requireNonNull(formattedConfigs, "Formatted config string can't be null");

            if (formattedConfigs.indexOf('=') == -1) {
                throw new IllegalStateException(string.format("Provided string [ %s ] does not have expected key-value separator of '='", formattedConfigs));
            }

            string[] parts = formattedConfigs.split(",");
            Dictionary<string, string> configs = new HashMap<>();
            foreach (string part in parts) {
                string[] keyValue = part.split("=");
                if (keyValue.Length > 2) {
                    throw new IllegalStateException(
                        string.format("Provided string [ %s ] does not have expected key-value pair separator of ','", formattedConfigs));
                }
                configs.put(keyValue[KEY], keyValue[VALUE]);
            }
            return configs;
        }
    }
}
