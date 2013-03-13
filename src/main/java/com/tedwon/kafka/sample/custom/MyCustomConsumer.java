package com.tedwon.kafka.sample.custom;

import com.tedwon.kafka.sample.ExampleUtils;
import com.tedwon.kafka.sample.KafkaProperties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class MyCustomConsumer extends Thread {

    /**
     * SLF4J Logger.
     */
    private static Logger logger = LoggerFactory.getLogger(MyCustomConsumer.class);

    private final ConsumerConnector consumer;
    private final String topic;

    private static boolean fromBeginning;

    long counter = 0L;

    public MyCustomConsumer(String topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig());
        this.topic = topic;
    }

    public MyCustomConsumer(String topic, boolean fromBeginning) {
        this.fromBeginning = fromBeginning;

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig());
        this.topic = topic;
    }


    public void shutdownConsumerConnector() {

        try {
            logger.info("Calling shutdownConsumerConnector ");
            consumer.commitOffsets();
            consumer.shutdown();
        } catch (Exception e) {
            logger.error("ConsumerConnector shutdown failed: ", e);
        }

        logger.info("### {} - {}", this.topic, this.counter);
    }

    private static ConsumerConfig createConsumerConfig() {

        Properties props = new Properties();
        props.put("zk.connect", KafkaProperties.zkConnect);




        if (fromBeginning) {
            // from-beginning
            props.put("groupid", KafkaProperties.groupId + new Random().nextInt(100000));
            props.put("zk.sessiontimeout.ms", "400");
            props.put("zk.synctime.ms", "200");
            props.put("autocommit.interval.ms", "1000");
            props.put("autooffset.reset", "smallest");
            props.put("auto.commit", "false");
            props.put("autocommit.enable", "false");
//            props.put("fetch.size", "102400");

        } else {
            // from-before
            props.put("groupid", KafkaProperties.groupId);
            props.put("zk.sessiontimeout.ms", "400");
            props.put("zk.synctime.ms", "200");
            props.put("autocommit.interval.ms", "1000");
            props.put("autooffset.reset", "largest");
//            props.put("auto.commit", "true");
//            props.put("autocommit.enable", "true");
//            props.put("fetch.size", "102400");
        }

        return new ConsumerConfig(props);
    }

    public void run() {



        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<Message>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<Message> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<Message> it = stream.iterator();
        while (it.hasNext()) {
//            logger.info("{}", ExampleUtils.getMessage(it.next().message()));
            it.next().message();
            counter++;

        }
    }
}
