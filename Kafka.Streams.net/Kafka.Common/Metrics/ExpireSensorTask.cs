using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace Kafka.Common.Metrics
{
    /**
     * This iterates over every Sensor and triggers a removeSensor if it has expired
     * Package private for testing
     */
    public class ExpireSensorTask
    {
        public void run()
        {
//            foreach (KeyValuePair<string, Sensor> sensorEntry in sensors.entrySet())
            {
                // removeSensor also locks the sensor object. This is fine because synchronized is reentrant
                // There is however a minor race condition here. Assume we have a parent sensor P and child sensor C.
                // Calling record on C would cause a record on P as well.
                // So expiration time for P == expiration time for C. If the record on P happens via C just after P is removed,
                // that will cause C to also get removed.
                // Since the expiration time is typically high it is not expected to be a significant concern
                // and thus not necessary to optimize
        //        lock (sensorEntry.Value)
        //        {
        //            if (sensorEntry.Value.hasExpired())
        //            {
        ////                log.LogDebug("Removing expired sensor {}", sensorEntry.getKey());
        //                //removeSensor(sensorEntry.Key);
        //            }
        //        }
            }
        }
    }
}