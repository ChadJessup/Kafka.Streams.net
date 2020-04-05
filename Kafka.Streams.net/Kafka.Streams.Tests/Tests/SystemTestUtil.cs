using Confluent.Kafka;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Tools
{
    /**
     * Class for common convenience methods for working on
     * System tests
     */

    public class SystemTestUtil
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
                throw new System.ArgumentException("message", nameof(formattedConfigs));
            }

            if (formattedConfigs.IndexOf('=') == -1)
            {
                throw new System.InvalidOperationException(string.Format("Provided string [ %s ] does not have expected key-value separator of '='", formattedConfigs));
            }

            string[] parts = formattedConfigs.Split(",");
            var configs = new Dictionary<string, string>();

            foreach (string part in parts)
            {
                string[] keyValue = part.Split("=");
                if (keyValue.Length > 2)
                {
                    throw new InvalidOperationException(
                        string.Format("Provided string [ %s ] does not have expected key-value pair separator of ','", formattedConfigs));
                }

                configs.Add(keyValue[KEY], keyValue[VALUE]);
            }
            return configs;
        }
    }
}
