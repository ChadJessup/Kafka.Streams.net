
//        public SuppressedInternal(
//            string name,
//            TimeSpan suppressionTime,
//            IBufferConfig bufferConfig,
//            ITimeDefinition<K, V> timeDefinition,
//            bool safeToDropTombstones)
//        {
//            this.name = name;
//            this._timeToWaitForMoreEvents = suppressionTime == null ? DEFAULT_SUPPRESSION_TIME : suppressionTime;
//            this.timeDefinition = timeDefinition == null ? RecordTimeDefintion<K, V>.instance() : timeDefinition;
//            this.bufferConfig = bufferConfig == null ? DEFAULT_BUFFER_CONFIG : (BufferConfigInternal)bufferConfig;
//            this.safeToDropTombstones = safeToDropTombstones;
//        }


//        public ISuppressed<K> withName(string name)
//        {
//            return new SuppressedInternal<K, V>(name, _timeToWaitForMoreEvents, bufferConfig, timeDefinition, safeToDropTombstones);
//        }


//        public string name { get; set; }

//        TimeSpan timeToWaitForMoreEvents()
//        {
//            return _timeToWaitForMoreEvents == null ? TimeSpan.Zero : _timeToWaitForMoreEvents;
//        }

//        public override bool Equals(object o)
//        {
//            if (this == o)
//            {
//                return true;
//            }
//            if (o == null || GetType() != o.GetType())
//            {
//                return false;
//            }

//            SuppressedInternal<K, object> that = (SuppressedInternal<K, object>)o;
//            return safeToDropTombstones == that.safeToDropTombstones &&
//                name.Equals(that.name) &&
//                bufferConfig.Equals(that.bufferConfig) &&
//                _timeToWaitForMoreEvents.Equals(that._timeToWaitForMoreEvents) &&
//                timeDefinition.Equals(that.timeDefinition);
//        }


//        public override int GetHashCode()
//        {
//            return (
//                    name,
//                    bufferConfig,
//                    _timeToWaitForMoreEvents,
//                    timeDefinition,
//                    safeToDropTombstones)
//                    .GetHashCode();
//        }


//        public string ToString()
//        {
//            return "SuppressedInternal{" +
//                    "name='" + name + '\'' +
//                    ", bufferConfig=" + bufferConfig +
//                    ", timeToWaitForMoreEvents=" + timeToWaitForMoreEvents +
//                    ", timeDefinition=" + timeDefinition +
//                    ", safeToDropTombstones=" + safeToDropTombstones +
//                    '}';
//        }
//    }
//}
