package org.apache.rocketmq.common.protocol.topic;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

public class OffsetMovedEventTest {

  @Test
  public void testFromJson() throws Exception {
    OffsetMovedEvent event = new OffsetMovedEvent();
    event.setConsumerGroup("test-group");
    event.setMessageQueue(new MessageQueue("test-topic", "test-broker", 0));
    event.setOffsetRequest(3000L);
    event.setOffsetNew(1000L);

    String json = OffsetMovedEvent.toJson(event, true);
    OffsetMovedEvent fromJson = OffsetMovedEvent.fromJson(json, OffsetMovedEvent.class);

    assertThat(fromJson.getConsumerGroup()).isEqualTo(event.getConsumerGroup());
    assertThat(fromJson.getMessageQueue().getTopic()).isEqualTo(event.getMessageQueue().getTopic());
    assertThat(fromJson.getMessageQueue().getBrokerName())
        .isEqualTo(event.getMessageQueue().getBrokerName());
    assertThat(fromJson.getMessageQueue().getQueueId())
        .isEqualTo(event.getMessageQueue().getQueueId());
    assertThat(fromJson.getOffsetRequest()).isEqualTo(event.getOffsetRequest());
    assertThat(fromJson.getOffsetNew()).isEqualTo(event.getOffsetNew());
  }
}
