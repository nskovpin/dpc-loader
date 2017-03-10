//package ru.at_consulting.bigdata.dpc.kafka;
//
//import io.confluent.kafka.serializers.KafkaAvroDeserializer;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.common.PartitionInfo;
//import org.apache.kafka.common.serialization.StringDeserializer;
//
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//
//import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
//import static java.util.Collections.singletonList;
//import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
//import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
//
///**
// * Created by NSkovpin on 27.02.2017.
// */
//public class LoadFromKafka {
//
//    public static void main(String[] args) {
//        LoadFromKafka loadFromKafka = new LoadFromKafka();
//        loadFromKafka.readFromKafka("kafka01.vimpelcom.ru:6667", "dpc", "glassfish_dpc");
//    }
//
//    public void readFromKafka(String url, String group, String topic){
//        Properties properties = getKafkaProperties(url, group);
//        KafkaConsumer<Object, GenericRecord> consumer = new KafkaConsumer<>(properties);
//
//        Map<String, List<PartitionInfo>> topics = consumer.listTopics();
//
//        consumer.subscribe(singletonList(topic));
//
//        ConsumerRecords consumerRecords = consumer.poll(100);
//        if(consumerRecords.isEmpty()){
//            System.out.println("empty");
//        }else{
//            System.out.println(consumerRecords.count());
//        }
//    }
//
//    public Properties getKafkaProperties(String url, String group){
//        Properties properties = new Properties();
//        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        properties.put(BOOTSTRAP_SERVERS_CONFIG, url);
//        properties.put(GROUP_ID_CONFIG, group);
//        properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
//        return properties;
//    }
//
//}
