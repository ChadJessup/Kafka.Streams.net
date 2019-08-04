using System;
using System.Collections.Generic;
using System.Globalization;

namespace Kafka.Common.Metrics
{
    public class RecordingLevel
    {
        public static RecordingLevel INFO = new RecordingLevel(0, "INFO");
        public static RecordingLevel DEBUG = new RecordingLevel(1, "DEBUG");

        private static Dictionary<string, RecordingLevel> levels = new Dictionary<string, RecordingLevel>();
        private static RecordingLevel[] ID_TO_TYPE;
        private static int MIN_RECORDING_LEVEL_KEY = 0;
        public static int MAX_RECORDING_LEVEL_KEY;

        int maxRL = -1;

        public RecordingLevel(int id, string name)
        {
            this.id = (short)id;
            this.name = name;
        }

        static RecordingLevel()
        {
            int maxRL = -1;
            levels.Add(INFO.name, INFO);
            levels.Add(DEBUG.name, DEBUG);

            foreach (RecordingLevel level in levels.Values)
            {
                maxRL = Math.Max(maxRL, level.id);
            }

            var idToName = new RecordingLevel[maxRL + 1];

            foreach (RecordingLevel level in levels.Values)
            {
                idToName[level.id] = level;
            }

            ID_TO_TYPE = idToName;
            MAX_RECORDING_LEVEL_KEY = maxRL;
        }

        /** an english description of the api--this is for debugging and can change */
        public string name { get; set; }

        /** the permanent and immutable id of an API--this can't change ever */
        public short id { get; }

        public static RecordingLevel forId(int id)
        {
            if (id < MIN_RECORDING_LEVEL_KEY || id > MAX_RECORDING_LEVEL_KEY)
            {
                throw new System.ArgumentException(string.Format("Unexpected RecordLevel id `%d`, it should be between `%d` " +
                    "and `%d` (inclusive)", id, MIN_RECORDING_LEVEL_KEY, MAX_RECORDING_LEVEL_KEY));
            }

            return ID_TO_TYPE[id];
        }

        /** Case insensitive lookup by protocol name */
        public static RecordingLevel forName(string name)
        {
            return levels[name.ToUpper(CultureInfo.InvariantCulture]);
        }

        public bool shouldRecord(int configId)
        {
            return configId == DEBUG.id || configId == this.id;
        }
    }
}