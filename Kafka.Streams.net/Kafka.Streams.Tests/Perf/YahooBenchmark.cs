namespace Kafka.Streams.Tests.Perf
{
    /*






    *

    *





    */


































    /**
     * A basic DSL and data generation that emulates the behavior of the Yahoo Benchmark
     * https://yahooeng.tumblr.com/post/135321837876/benchmarking-streaming-computation-engines-at
     * Thanks to Michael Armbrust for providing the initial code for this benchmark in his blog:
     * https://databricks.com/blog/2017/06/06/simple-super-fast-streaming-engine-apache-spark.html
     */
    public class YahooBenchmark
    {
        private readonly SimpleBenchmark parent;
        private readonly string campaignsTopic;
        private readonly string eventsTopic;

        static class ProjectedEvent
        {
            /* attributes need to be public for serializer to work */
            /* main attributes */
            readonly string eventType;
            readonly string adID;

            /* other attributes */
            readonly long eventTime;
            /* not used
            public string userID = UUID.randomUUID().toString();
            public string pageID = UUID.randomUUID().toString();
            public string addType = "banner78";
            public string ipAddress = "1.2.3.4";
             */
        }

        static class CampaignAd
        {
            /* attributes need to be public for serializer to work */
            readonly string adID;
            readonly string campaignID;
        }


        public YahooBenchmark(SimpleBenchmark parent, string campaignsTopic, string eventsTopic)
        {
            this.parent = parent;
            this.campaignsTopic = campaignsTopic;
            this.eventsTopic = eventsTopic;
        }

        // just for Yahoo benchmark
        private bool MaybeSetupPhaseCampaigns(string topic,
                                                 string clientId,
                                                 bool skipIfAllTests,
                                                 int numCampaigns,
                                                 int adsPerCampaign,
                                                 List<string> ads)
        {
            parent.resetStats();
            // initialize topics
            System.Console.Out.WriteLine("Initializing topic " + topic);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, parent.props.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer);

            try
            {
                (KafkaProducer<string, string> producer = new KafkaProducer<>(props));
                for (int c = 0; c < numCampaigns; c++)
                {
                    string campaignID = UUID.randomUUID().toString();
                    for (int a = 0; a < adsPerCampaign; a++)
                    {
                        string adId = UUID.randomUUID().toString();
                        string concat = adId + ":" + campaignID;
                        producer.send(new ProducerRecord<>(topic, adId, concat));
                        ads.add(adId);
                        parent.processedRecords++;
                        parent.processedBytes += concat.Length() + adId.Length();
                    }
                }
            }
        return true;
        }

        // just for Yahoo benchmark
        private void MaybeSetupPhaseEvents(string topic,
                                           string clientId,
                                           int numRecords,
                                           List<string> ads)
        {
            parent.resetStats();
            string[] eventTypes = new string[] { "view", "click", "purchase" };
            Random rand = new Random(System.currentTimeMillis());
            System.Console.Out.WriteLine("Initializing topic " + topic);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, parent.props.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer);

            long startTime = System.currentTimeMillis();

            try
            {
                (KafkaProducer<string, byte[]> producer = new KafkaProducer<>(props));
                ProjectedEvent event = new ProjectedEvent();
        Dictionary<string, object> serdeProps = new HashMap<>();
        Serializer<ProjectedEvent> projectedEventSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", ProjectedEvent);
            projectedEventSerializer.configure(serdeProps, false);

            for (int i = 0; i<numRecords; i++) {
                event.eventType = eventTypes [rand.nextInt(eventTypes.Length - 1)];
                event.adID = ads.get(rand.nextInt(ads.Count - 1));
                event.eventTime = System.currentTimeMillis();
                readonly byte[] value = projectedEventSerializer.serialize(topic, event);
        producer.send(new ProducerRecord<>(topic, event.adID, value));
        parent.processedRecords++;
        parent.processedBytes += value.Length + event.adID.Length();
            }
        }

        long endTime = System.currentTimeMillis();

    parent.printResults("Producer Performance [records/latency/rec-sec/MB-sec write]: ", endTime - startTime);
    }


    public void Run()
    {
        int numCampaigns = 100;
        int adsPerCampaign = 10;

        List<string> ads = new ArrayList<>(numCampaigns * adsPerCampaign);
        maybeSetupPhaseCampaigns(campaignsTopic, "simple-benchmark-produce-campaigns", false, numCampaigns, adsPerCampaign, ads);
        maybeSetupPhaseEvents(eventsTopic, "simple-benchmark-produce-events", parent.numRecords, ads);

        CountDownLatch latch = new CountDownLatch(1);
        parent.setStreamProperties("simple-benchmark-yahoo" + new Random().nextInt());

        KafkaStreams streams = createYahooBenchmarkStreams(parent.props, campaignsTopic, eventsTopic, latch, parent.numRecords);
        parent.runGenericBenchmark(streams, "Streams Yahoo Performance [records/latency/rec-sec/MB-sec counted]: ", latch);

    }
    // Note: these are also in the streams example package, eventually use 1 file
    private class JsonPOJOSerializer<T> : Serializer<T>
    {
        private ObjectMapper objectMapper = new ObjectMapper();

        /**
         * Default constructor needed by Kafka
         */

        public JsonPOJOSerializer() { }


        public byte[] Serialize(string topic, T data)
        {
            if (data == null)
            {
                return null;
            }

            try
            {
                return objectMapper.writeValueAsBytes(data);
            }
            catch (Exception e)
            {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }
    }

    // Note: these are also in the streams example package, eventuall use 1 file
    private class JsonPOJODeserializer<T> : Deserializer<T>
    {
        private ObjectMapper objectMapper = new ObjectMapper();

        private Class<T> tClass;

        /**
         * Default constructor needed by Kafka
         */

        public JsonPOJODeserializer() { }



        public void Configure(Dictionary<string, ?> props, bool isKey)
        {
            tClass = (Class<T>)props.get("JsonPOJOClass");
        }


        public T Deserialize(string topic, byte[] bytes)
        {
            if (bytes == null)
            {
                return null;
            }

            T data;
            try
            {
                data = objectMapper.readValue(bytes, tClass);
            }
            catch (Exception e)
            {
                throw new SerializationException(e);
            }

            return data;
        }
    }

    private KafkaStreams CreateYahooBenchmarkStreams(Properties streamConfig, string campaignsTopic, string eventsTopic,
                                                     CountDownLatch latch, int numRecords)
    {
        Dictionary<string, object> serdeProps = new HashMap<>();
        Serializer<ProjectedEvent> projectedEventSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", ProjectedEvent);
        projectedEventSerializer.configure(serdeProps, false);
        Deserializer<ProjectedEvent> projectedEventDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", ProjectedEvent);
        projectedEventDeserializer.configure(serdeProps, false);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<string, ProjectedEvent> kEvents = builder.stream(eventsTopic,
                                                                       Consumed.with(Serdes.String(),
                                                                                     Serdes.serdeFrom(projectedEventSerializer, projectedEventDeserializer)));
        KTable<string, string> kCampaigns = builder.table(campaignsTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<string, ProjectedEvent> filteredEvents = kEvents
            // use peek to quick when last element is processed
            .peek((key, value) =>
            {
                parent.processedRecords++;
                if (parent.processedRecords % 1000000 == 0)
                {
                    System.Console.Out.WriteLine("Processed " + parent.processedRecords);
                }
                if (parent.processedRecords >= numRecords)
                {
                    latch.countDown();
                }
            })
            // only keep "view" events
            .filter((key, value) => value.eventType.equals("view"))
            // select just a few of the columns
            .mapValues(value =>
            {
                ProjectedEvent event = new ProjectedEvent();
                event.adID = value.adID;
                event.eventTime = value.eventTime;
                event.eventType = value.eventType;
                return event;
            });

        // deserialize the add ID and campaign ID from the stored value in Kafka
        KTable<string, CampaignAd> deserCampaigns = kCampaigns.mapValues(value =>
        {
            string[] parts = value.split(":");
            CampaignAd cAdd = new CampaignAd();
            cAdd.adID = parts[0];
            cAdd.campaignID = parts[1];
            return cAdd;
        });

        // join the events with the campaigns
        KStream<string, string> joined = filteredEvents.join(
            deserCampaigns,
            (value1, value2) => value2.campaignID,
            Joined.with(Serdes.String(), Serdes.serdeFrom(projectedEventSerializer, projectedEventDeserializer), null)
        );

        // key by campaign rather than by ad as original
        KStream<string, string> keyedByCampaign = joined
            .selectKey((key, value) => value);

        // calculate windowed counts
        keyedByCampaign
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.of(Duration.ofMillis(10 * 1000)))
            .count(Materialized.As("time-windows"));

        return new KafkaStreams(builder.build(), streamConfig);
    }
}
}
/*






*

*





*/


































/**
 * A basic DSL and data generation that emulates the behavior of the Yahoo Benchmark
 * https://yahooeng.tumblr.com/post/135321837876/benchmarking-streaming-computation-engines-at
 * Thanks to Michael Armbrust for providing the initial code for this benchmark in his blog:
 * https://databricks.com/blog/2017/06/06/simple-super-fast-streaming-engine-apache-spark.html
 */




// Note: these are also in the streams example package, eventually use 1 file

// Note: these are also in the streams example package, eventuall use 1 file

