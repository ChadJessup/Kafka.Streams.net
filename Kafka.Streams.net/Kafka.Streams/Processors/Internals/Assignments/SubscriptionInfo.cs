
//        public static SubscriptionInfo decode(ByteBuffer data)
//        {
//            SubscriptionInfo subscriptionInfo;

//            // ensure we are at the beginning of the ByteBuffer
//            data.rewind();

//            int usedVersion = data.getInt();
//            int latestSupportedVersion;
//            switch (usedVersion)
//            {
//                case 1:
//                    subscriptionInfo = new SubscriptionInfo(usedVersion, UNKNOWN);
//                    decodeVersionOneData(subscriptionInfo, data);
//                    break;
//                case 2:
//                    subscriptionInfo = new SubscriptionInfo(usedVersion, UNKNOWN);
//                    decodeVersionTwoData(subscriptionInfo, data);
//                    break;
//                case 3:
//                case 4:
//                    latestSupportedVersion = data.getInt();
//                    subscriptionInfo = new SubscriptionInfo(usedVersion, latestSupportedVersion);
//                    decodeVersionThreeData(subscriptionInfo, data);
//                    break;
//                default:
//                    latestSupportedVersion = data.getInt();
//                    subscriptionInfo = new SubscriptionInfo(usedVersion, latestSupportedVersion);
//                    log.LogInformation("Unable to decode subscription data: used version: {}; latest supported version: {}", usedVersion, LATEST_SUPPORTED_VERSION);
//                    break;
//            }

//            return subscriptionInfo;
//        }

//        private static void decodeVersionOneData(SubscriptionInfo subscriptionInfo,
//                                                 ByteBuffer data)
//        {
//            decodeClientUUID(subscriptionInfo, data);
//            decodeTasks(subscriptionInfo, data);
//        }

//        private static void decodeClientUUID(SubscriptionInfo subscriptionInfo,
//                                             ByteBuffer data)
//        {
//            subscriptionInfo.processId = new Guid(data.getLong(), data.getLong());
//        }

//        private static void decodeTasks(SubscriptionInfo subscriptionInfo,
//                                        ByteBuffer data)
//        {
//            subscriptionInfo.prevTasks = new HashSet<>();
//            int numPrevTasks = data.getInt();
//            for (int i = 0; i < numPrevTasks; i++)
//            {
//                subscriptionInfo.prevTasks.Add(TaskId.readFrom(data));
//            }

//            subscriptionInfo.standbyTasks = new HashSet<>();
//            int numStandbyTasks = data.getInt();
//            for (int i = 0; i < numStandbyTasks; i++)
//            {
//                subscriptionInfo.standbyTasks.Add(TaskId.readFrom(data));
//            }
//        }

//        private static void decodeVersionTwoData(SubscriptionInfo subscriptionInfo,
//                                                 ByteBuffer data)
//        {
//            decodeClientUUID(subscriptionInfo, data);
//            decodeTasks(subscriptionInfo, data);
//            decodeUserEndPoint(subscriptionInfo, data);
//        }

//        private static void decodeUserEndPoint(SubscriptionInfo subscriptionInfo,
//                                               ByteBuffer data)
//        {
//            int bytesLength = data.getInt();
//            if (bytesLength != 0)
//            {
//                byte[] bytes = new byte[bytesLength];
//                data.Get(bytes);
//                subscriptionInfo.userEndPoint = new string(bytes, System.Text.Encoding.UTF8);
//            }
//        }

//        private static void decodeVersionThreeData(SubscriptionInfo subscriptionInfo,
//                                                   ByteBuffer data)
//        {
//            decodeClientUUID(subscriptionInfo, data);
//            decodeTasks(subscriptionInfo, data);
//            decodeUserEndPoint(subscriptionInfo, data);
//        }


//        public int GetHashCode()
//        {
//            int hash = usedVersion ^ latestSupportedVersion ^ processId.GetHashCode() ^ prevTasks.GetHashCode() ^ standbyTasks.GetHashCode();
//            if (userEndPoint == null)
//            {
//                return GetHashCode();
//            }
//            return GetHashCode() ^ userEndPoint.GetHashCode();
//        }


//        public bool Equals(object o)
//        {
//            if (o is SubscriptionInfo)
//            {
//                SubscriptionInfo other = (SubscriptionInfo)o;
//                return this.usedVersion == other.usedVersion &&
//                        this.latestSupportedVersion == other.latestSupportedVersion &&
//                        this.processId.Equals(other.processId) &&
//                        this.prevTasks.Equals(other.prevTasks) &&
//                        this.standbyTasks.Equals(other.standbyTasks) &&
//                        this.userEndPoint != null ? this.userEndPoint.Equals(other.userEndPoint) : other.userEndPoint == null;
//            }
//            else
//            {

//                return false;
//            }
//        }


//        public string ToString()
//        {
//            return "[version=" + usedVersion
//                + ", supported version=" + latestSupportedVersion
//                + ", process ID=" + processId
//                + ", prev tasks=" + prevTasks
//                + ", standby tasks=" + standbyTasks
//                + ", user endpoint=" + userEndPoint + "]";
//        }
//    }
//}