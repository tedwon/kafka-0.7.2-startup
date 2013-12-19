package com.tedwon.kafka.sample.custom;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

/**
 * MyCustomProducer Class<p/>
 *
 * @author <a href=mailto:iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class MyCustomProducer {

  private final kafka.javaapi.producer.Producer<Integer, String> producer;
  private final String topic;
  private final Properties props = new Properties();

  public MyCustomProducer(String topic) {
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("zk.connect", "localhost:2181");
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
    this.topic = topic;
  }

  public void run() {
    int messageNo = 1;
    while (true) {
      String messageStr = new String("Message_" + messageNo);
      List list = new ArrayList();
      list.add(messageStr);
      producer.send(new ProducerData<Integer, String>(topic, messageNo, list));
      System.out.println(messageStr);
      messageNo++;
//      if(messageNo == 100001) {
//        break;
//      }
      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {

      }
    }
  }

  public static void main(String[] args) {
    String topic = args[0];
    MyCustomProducer producer = new MyCustomProducer(topic);
    producer.run();
  }

}
