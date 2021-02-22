import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.math.*;

public class producer {
    public producer() throws InterruptedException {
        String inputTopic = "primeInput";
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "prime-number-consumer");
        String bootstrapServers = "localhost:9092";
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(inputTopic);
        //textLines.foreach((key,value)->System.out.println("hello"+value));
        KStream<String,String> prime= textLines.flatMap(
                (key,value)->{
                    List<KeyValue<String,String>> result=new LinkedList<>();
                    String x=value;
                    if(isPrime(Integer.valueOf(x))){
                        result.add(KeyValue.pair(key,"prime"));
                    }
                    return result;
                }
        );

        prime.foreach((key,value)->System.out.println(key+" "+value));

        String outputTopic="prime";
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        prime.to(outputTopic);

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        Thread.sleep(30000);
        streams.close();
    }

    public static Boolean isPrime(int n){
        BigInteger b=new BigInteger(String.valueOf(n));
        return b.isProbablePrime(1);
    }
}
