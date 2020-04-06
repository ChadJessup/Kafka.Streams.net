//using System;
//using System.Threading;

//namespace Kafka.Streams.Tests.Integration
//{
//    public class SmokeTestDriverIntegrationTest
//    {

//        //public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

//        private class Driver // : Thread
//        {
//            private readonly string bootstrapServers;
//            private readonly int numKeys;
//            private readonly int maxRecordsPerKey;
//            private Exception exception = null;
//            private SmokeTestDriver.VerificationResult result;

//            private Driver(string bootstrapServers, int numKeys, int maxRecordsPerKey)
//            {
//                this.bootstrapServers = bootstrapServers;
//                this.numKeys = numKeys;
//                this.maxRecordsPerKey = maxRecordsPerKey;
//            }


//            public void Run()
//            {
//                try
//                {
//                    Dictionary<string, HashSet<int>> allData =
//                        generate(bootstrapServers, numKeys, maxRecordsPerKey, TimeSpan.ofSeconds(20));
//                    result = verify(bootstrapServers, allData, maxRecordsPerKey);

//                }
//                catch (Exception ex)
//                {
//                    this.exception = ex;
//                }
//            }

//            public Exception Exception()
//            {
//                return exception;
//            }

//            SmokeTestDriver.VerificationResult Result()
//            {
//                return result;
//            }

//        }

//        [Fact]
//        public void ShouldWorkWithRebalance()
//        {// throws InterruptedException
//            int numClientsCreated = 0;
//            ArrayList<SmokeTestClient> clients = new ArrayList<>();

//            IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, SmokeTestDriver.topics());

//            string bootstrapServers = CLUSTER.bootstrapServers();
//            Driver driver = new Driver(bootstrapServers, 10, 1000);
//            driver.start();
//            System.Console.Out.WriteLine("started driver");


//            StreamsConfig props = new StreamsConfig();
//            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

//            // cycle out Streams instances as long as the test is running.
//            while (driver.isAlive())
//            {
//                // take a nap
//                Thread.sleep(1000);

//                // add a new client
//                SmokeTestClient smokeTestClient = new SmokeTestClient("streams-" + numClientsCreated++);
//                clients.Add(smokeTestClient);
//                smokeTestClient.start(props);

//                while (!clients.Get(clients.Count - 1).started())
//                {
//                    Thread.sleep(100);
//                }

//                // let the oldest client die of "natural causes"
//                if (clients.Count >= 3)
//                {
//                    clients.remove(0).closeAsync();
//                }
//            }
//            try
//            {
//                // wait for verification to finish
//                driver.join();


//            }
//            finally
//            {
//                // whether or not the assertions failed, tell all the streams instances to stop
//                foreach (SmokeTestClient client in clients)
//                {
//                    client.closeAsync();
//                }

//                // then, wait for them to stop
//                foreach (SmokeTestClient client in clients)
//                {
//                    client.close();
//                }
//            }

//            // check to make sure that it actually succeeded
//            if (driver.Exception() != null)
//            {
//                driver.Exception().printStackTrace();
//                throw new AssertionError(driver.Exception());
//            }
//            Assert.True(driver.result().result(), driver.result().passed());
//        }

//    }
//}
///*






//*

//*





//*/























