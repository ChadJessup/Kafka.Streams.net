/*






 *

 *





 */








class SerdeThatDoesntHandleNull : Serde<string> {
    
    public Serializer<string> Serializer() {
        return new StringSerializer();
    }

    
    public Deserializer<string> Deserializer() {
        return new StringDeserializer() {
            
            public string deserialize(string topic, byte[] data) {
                if (data == null) {
                    throw new NullPointerException();
                }
                return base.deserialize(topic, data);
            }
        };
    }
}
