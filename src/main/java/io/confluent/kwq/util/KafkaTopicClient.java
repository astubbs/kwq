package io.confluent.kwq.util;

/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

import org.apache.kafka.clients.admin.TopicDescription;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface KafkaTopicClient extends Closeable {

  /**
   * Create a new topic with the specified name, numPartitions and replicatonFactor.
   * [warn] synchronous call to get the response
   *
   * @param topic name of the topic to create
   */
  void createTopic(String topic, int numPartitions, short replicatonFactor);

  /**
   * Create a new topic with the specified name, numPartitions and replicatonFactor.
   * [warn] synchronous call to get the response
   *
   * @param topic name of the topic to create
   * @param configs any additional topic configs to use
   */
  void createTopic(
          String topic,
          int numPartitions,
          short replicatonFactor,
          Map<String, String> configs
  );

  /**
   * [warn] synchronous call to get the response
   *
   * @param topic name of the topic
   * @return whether the topic exists or not
   */
  boolean isTopicExists(String topic);

  /**
   * [warn] synchronous call to get the response
   *
   * @return set of existing topic names
   */
  Set<String> listTopicNames();

  /**
   * [warn] synchronous call to get the response
   *
   * @param topicNames topicNames to describe
   */
  Map<String, TopicDescription> describeTopics(Collection<String> topicNames);

  /**
   * Delete the list of the topics in the given list.
   */
  void deleteTopics(List<String> topicsToDelete);

  /**
   * Delete the internal topics of a given application.
   */
  void deleteInternalTopics(String applicationId);

  /**
   * Close the underlying Kafka admin client.
   */
  void close();

}