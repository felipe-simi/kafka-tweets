package com.simi.studies.kafkatweets.infra.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaTweetsProducer {

  private final KafkaTemplate<String, Object> producer;

  public KafkaTweetsProducer(final KafkaTemplate<String, Object> producer) {
    this.producer = producer;
  }

  public void publish(final Object tweet) {
    producer.send("tweet_received", tweet);
  }

}
