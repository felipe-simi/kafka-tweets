package com.simi.studies.kafkatweets.infra.twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Component
public class TwitterListener {


  private final String apiKey;
  private final String apiSecret;
  private final String accessToken;
  private final String accessSecret;

  public TwitterListener(@Value("${application.twitter.api-key}") final String apiKey,
                         @Value("${application.twitter.api-secret}") final String apiSecret,
                         @Value("${application.twitter.access-token}") final String accessToken,
                         @Value("${application.twitter.access-secret}") final String accessSecret) {
    this.apiKey = apiKey;
    this.apiSecret = apiSecret;
    this.accessToken = accessToken;
    this.accessSecret = accessSecret;
    startListening();
  }

  private void startListening() {
    final var msgQueue = new LinkedBlockingQueue<String>(1000);
    final var twitterClient = createClient(msgQueue, List.of("kotlin", "java"));
    twitterClient.connect();
    while (!twitterClient.isDone()) {
      try {
        final var msg = msgQueue.poll(1, TimeUnit.MINUTES);
        System.out.println(msg);
      } catch (InterruptedException e) {
        twitterClient.stop();
        throw new RuntimeException(e);
      }
    }
  }

  private Client createClient(final LinkedBlockingQueue<String> msgQueue,
                              final List<String> terms) {
    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    final var hosebirdAuth = new OAuth1(apiKey, apiSecret, accessToken, accessSecret);
    final var hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    final var hosebirdEndpoint = new StatusesFilterEndpoint();
    hosebirdEndpoint.trackTerms(terms);
    return new ClientBuilder()
        .name("Hosebird-Client-01")
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hosebirdEndpoint)
        .processor(new StringDelimitedProcessor(msgQueue))
        .build();
  }

}
