/*






 *

 *





 */























public class SmokeTestDriverIntegrationTest {
    
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);


    private static class Driver : Thread {
        private string bootstrapServers;
        private int numKeys;
        private int maxRecordsPerKey;
        private Exception exception = null;
        private SmokeTestDriver.VerificationResult result;

        private Driver(string bootstrapServers, int numKeys, int maxRecordsPerKey) {
            this.bootstrapServers = bootstrapServers;
            this.numKeys = numKeys;
            this.maxRecordsPerKey = maxRecordsPerKey;
        }

        
        public void run() {
            try {
                Dictionary<string, HashSet<int>> allData =
                    generate(bootstrapServers, numKeys, maxRecordsPerKey, Duration.ofSeconds(20));
                result = verify(bootstrapServers, allData, maxRecordsPerKey);

            } catch (Exception ex) {
                this.exception = ex;
            }
        }

        public Exception exception() {
            return exception;
        }

        SmokeTestDriver.VerificationResult result() {
            return result;
        }

    }

    [Xunit.Fact]
    public void shouldWorkWithRebalance() {// throws InterruptedException
        int numClientsCreated = 0;
        ArrayList<SmokeTestClient> clients = new ArrayList<>();

        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, SmokeTestDriver.topics());

        string bootstrapServers = CLUSTER.bootstrapServers();
        Driver driver = new Driver(bootstrapServers, 10, 1000);
        driver.start();
        System.Console.Out.WriteLine("started driver");


        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // cycle out Streams instances as long as the test is running.
        while (driver.isAlive()) {
            // take a nap
            Thread.sleep(1000);

            // add a new client
            SmokeTestClient smokeTestClient = new SmokeTestClient("streams-" + numClientsCreated++);
            clients.add(smokeTestClient);
            smokeTestClient.start(props);

            while (!clients.get(clients.Count - 1).started()) {
                Thread.sleep(100);
            }

            // let the oldest client die of "natural causes"
            if (clients.Count >= 3) {
                clients.remove(0).closeAsync();
            }
        }
        try {
            // wait for verification to finish
            driver.join();


        } finally {
            // whether or not the assertions failed, tell all the streams instances to stop
            foreach (SmokeTestClient client in clients) {
                client.closeAsync();
            }

            // then, wait for them to stop
            foreach (SmokeTestClient client in clients) {
                client.close();
            }
        }

        // check to make sure that it actually succeeded
        if (driver.exception() != null) {
            driver.exception().printStackTrace();
            throw new AssertionError(driver.exception());
        }
        Assert.True(driver.result().result(), driver.result().passed());
    }

}
