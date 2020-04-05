using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Tools
{
    /**
     * Class for common convenience methods for working on
     * System tests
     */
    public static class SystemTestUtil
    {
        private const int KEY = 0;
        private const int VALUE = 1;

        /**
         * Takes a string with keys and values separated by '=' and each key value pair
         * separated by ',' for example max.block.ms=5000,retries=6,request.timeout.ms=6000
         *
         * This class makes it easier to pass configs from the system test in python to the Java test.
         *
         * @param formattedConfigs the formatted config string
         * @return HashMap with keys and values inserted
         */
        public static Dictionary<string, string> ParseConfigs(string formattedConfigs)
        {
            if (string.IsNullOrWhiteSpace(formattedConfigs))
            {
                throw new ArgumentNullException(nameof(formattedConfigs));
            }

            if (formattedConfigs.IndexOf('=') == -1)
            {
                throw new ArgumentException($"Provided string [ {formattedConfigs} ] does not have expected key-value separator of '='");
            }

            string[] parts = formattedConfigs.Split(",");
            var configs = new Dictionary<string, string>();

            foreach (string part in parts)
            {
                string[] keyValue = part.Split("=");
                if (keyValue.Length > 2)
                {
                    throw new ArgumentException(
                        $"Provided string [ {formattedConfigs} ] does not have expected key-value pair separator of ','",
                        nameof(formattedConfigs));
                }

                configs.Add(keyValue[KEY], keyValue[VALUE]);
            }

            return configs;
        }
    }
}
