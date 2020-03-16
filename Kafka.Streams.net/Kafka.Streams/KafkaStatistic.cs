using Newtonsoft.Json;

namespace Kafka.Streams
{
    public class IntLatency
    {
        [JsonProperty("min")]
        public int Min { get; set; }

        [JsonProperty("max")]
        public int Max { get; set; }

        [JsonProperty("avg")]
        public int Avg { get; set; }

        [JsonProperty("sum")]
        public int Sum { get; set; }

        [JsonProperty("stddev")]
        public int Stddev { get; set; }

        [JsonProperty("p50")]
        public int P50 { get; set; }

        [JsonProperty("p75")]
        public int P75 { get; set; }

        [JsonProperty("p90")]
        public int P90 { get; set; }

        [JsonProperty("p95")]
        public int P95 { get; set; }

        [JsonProperty("p99")]
        public int P99 { get; set; }

        [JsonProperty("p99_99")]
        public int P9999 { get; set; }

        [JsonProperty("outofrange")]
        public int Outofrange { get; set; }

        [JsonProperty("hdrsize")]
        public int Hdrsize { get; set; }

        [JsonProperty("cnt")]
        public int Cnt { get; set; }
    }

    public class OutbufLatency
    {

        [JsonProperty("min")]
        public int Min { get; set; }

        [JsonProperty("max")]
        public int Max { get; set; }

        [JsonProperty("avg")]
        public int Avg { get; set; }

        [JsonProperty("sum")]
        public int Sum { get; set; }

        [JsonProperty("stddev")]
        public int Stddev { get; set; }

        [JsonProperty("p50")]
        public int P50 { get; set; }

        [JsonProperty("p75")]
        public int P75 { get; set; }

        [JsonProperty("p90")]
        public int P90 { get; set; }

        [JsonProperty("p95")]
        public int P95 { get; set; }

        [JsonProperty("p99")]
        public int P99 { get; set; }

        [JsonProperty("p99_99")]
        public int P9999 { get; set; }

        [JsonProperty("outofrange")]
        public int Outofrange { get; set; }

        [JsonProperty("hdrsize")]
        public int Hdrsize { get; set; }

        [JsonProperty("cnt")]
        public int Cnt { get; set; }
    }

    public class Rtt
    {

        [JsonProperty("min")]
        public int Min { get; set; }

        [JsonProperty("max")]
        public int Max { get; set; }

        [JsonProperty("avg")]
        public int Avg { get; set; }

        [JsonProperty("sum")]
        public int Sum { get; set; }

        [JsonProperty("stddev")]
        public int Stddev { get; set; }

        [JsonProperty("p50")]
        public int P50 { get; set; }

        [JsonProperty("p75")]
        public int P75 { get; set; }

        [JsonProperty("p90")]
        public int P90 { get; set; }

        [JsonProperty("p95")]
        public int P95 { get; set; }

        [JsonProperty("p99")]
        public int P99 { get; set; }

        [JsonProperty("p99_99")]
        public int P9999 { get; set; }

        [JsonProperty("outofrange")]
        public int Outofrange { get; set; }

        [JsonProperty("hdrsize")]
        public int Hdrsize { get; set; }

        [JsonProperty("cnt")]
        public int Cnt { get; set; }
    }

    public class Throttle
    {

        [JsonProperty("min")]
        public int Min { get; set; }

        [JsonProperty("max")]
        public int Max { get; set; }

        [JsonProperty("avg")]
        public int Avg { get; set; }

        [JsonProperty("sum")]
        public int Sum { get; set; }

        [JsonProperty("stddev")]
        public int Stddev { get; set; }

        [JsonProperty("p50")]
        public int P50 { get; set; }

        [JsonProperty("p75")]
        public int P75 { get; set; }

        [JsonProperty("p90")]
        public int P90 { get; set; }

        [JsonProperty("p95")]
        public int P95 { get; set; }

        [JsonProperty("p99")]
        public int P99 { get; set; }

        [JsonProperty("p99_99")]
        public int P9999 { get; set; }

        [JsonProperty("outofrange")]
        public int Outofrange { get; set; }

        [JsonProperty("hdrsize")]
        public int Hdrsize { get; set; }

        [JsonProperty("cnt")]
        public int Cnt { get; set; }
    }

    public class Req
    {

        [JsonProperty("Produce")]
        public int Produce { get; set; }

        [JsonProperty("Offset")]
        public int Offset { get; set; }

        [JsonProperty("Metadata")]
        public int Metadata { get; set; }

        [JsonProperty("SaslHandshake")]
        public int SaslHandshake { get; set; }

        [JsonProperty("ApiVersion")]
        public int ApiVersion { get; set; }

        [JsonProperty("InitProducerId")]
        public int InitProducerId { get; set; }
    }

    public class Toppars
    {
    }

    public class Localhost9092Bootstrap
    {

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("nodeid")]
        public int Nodeid { get; set; }

        [JsonProperty("nodename")]
        public string Nodename { get; set; }

        [JsonProperty("source")]
        public string Source { get; set; }

        [JsonProperty("state")]
        public string State { get; set; }

        [JsonProperty("stateage")]
        public int Stateage { get; set; }

        [JsonProperty("outbuf_cnt")]
        public int OutbufCnt { get; set; }

        [JsonProperty("outbuf_msg_cnt")]
        public int OutbufMsgCnt { get; set; }

        [JsonProperty("waitresp_cnt")]
        public int WaitrespCnt { get; set; }

        [JsonProperty("waitresp_msg_cnt")]
        public int WaitrespMsgCnt { get; set; }

        [JsonProperty("tx")]
        public int Tx { get; set; }

        [JsonProperty("txbytes")]
        public int Txbytes { get; set; }

        [JsonProperty("txerrs")]
        public int Txerrs { get; set; }

        [JsonProperty("txretries")]
        public int Txretries { get; set; }

        [JsonProperty("req_timeouts")]
        public int ReqTimeouts { get; set; }

        [JsonProperty("rx")]
        public int Rx { get; set; }

        [JsonProperty("rxbytes")]
        public int Rxbytes { get; set; }

        [JsonProperty("rxerrs")]
        public int Rxerrs { get; set; }

        [JsonProperty("rxcorriderrs")]
        public int Rxcorriderrs { get; set; }

        [JsonProperty("rxpartial")]
        public int Rxpartial { get; set; }

        [JsonProperty("zbuf_grow")]
        public int ZbufGrow { get; set; }

        [JsonProperty("buf_grow")]
        public int BufGrow { get; set; }

        [JsonProperty("wakeups")]
        public int Wakeups { get; set; }

        [JsonProperty("connects")]
        public int Connects { get; set; }

        [JsonProperty("disconnects")]
        public int Disconnects { get; set; }

        [JsonProperty("int_latency")]
        public IntLatency IntLatency { get; set; }

        [JsonProperty("outbuf_latency")]
        public OutbufLatency OutbufLatency { get; set; }

        [JsonProperty("rtt")]
        public Rtt Rtt { get; set; }

        [JsonProperty("throttle")]
        public Throttle Throttle { get; set; }

        [JsonProperty("req")]
        public Req Req { get; set; }

        [JsonProperty("toppars")]
        public Toppars Toppars { get; set; }
    }

    public class Brokers
    {

        [JsonProperty("localhost:9092/bootstrap")]
        public Localhost9092Bootstrap Localhost9092Bootstrap { get; set; }
    }

    public class Topics
    {
    }

    public class KafkaStatistics
    {

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("client_id")]
        public string ClientId { get; set; }

        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("ts")]
        public long Ts { get; set; }

        [JsonProperty("time")]
        public int Time { get; set; }

        [JsonProperty("replyq")]
        public int Replyq { get; set; }

        [JsonProperty("msg_cnt")]
        public int MsgCnt { get; set; }

        [JsonProperty("msg_size")]
        public int MsgSize { get; set; }

        [JsonProperty("msg_max")]
        public int MsgMax { get; set; }

        [JsonProperty("msg_size_max")]
        public int MsgSizeMax { get; set; }

        [JsonProperty("simple_cnt")]
        public int SimpleCnt { get; set; }

        [JsonProperty("metadata_cache_cnt")]
        public int MetadataCacheCnt { get; set; }

        [JsonProperty("brokers")]
        public Brokers Brokers { get; set; }

        [JsonProperty("topics")]
        public Topics Topics { get; set; }

        [JsonProperty("tx")]
        public int Tx { get; set; }

        [JsonProperty("tx_bytes")]
        public int TxBytes { get; set; }

        [JsonProperty("rx")]
        public int Rx { get; set; }

        [JsonProperty("rx_bytes")]
        public int RxBytes { get; set; }

        [JsonProperty("txmsgs")]
        public int Txmsgs { get; set; }

        [JsonProperty("txmsg_bytes")]
        public int TxmsgBytes { get; set; }

        [JsonProperty("rxmsgs")]
        public int Rxmsgs { get; set; }

        [JsonProperty("rxmsg_bytes")]
        public int RxmsgBytes { get; set; }
    }
}
