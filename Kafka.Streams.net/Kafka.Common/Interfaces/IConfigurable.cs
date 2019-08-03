using System.Collections.Generic;

namespace Kafka.Common.Interfaces
{
    /**
     * A Mix-in style interface for classes that are instantiated by reflection and need to take configuration parameters
     */
    public interface IConfigurable {

        /**
         * Configure this class with the given key-value pairs
         */
        void configure(Dictionary<string, object> configs);

    }
}