using System.Collections.Generic;

namespace Confluent.Kafka
{
    public static class ConfigExtensions
    {
        public static Config SetAll(this Config config, Dictionary<string, string> props)
        {
            foreach (var prop in props)
            {
                config.Set(prop.Key, prop.Value);
            }

            return config;
        }
    }
}
