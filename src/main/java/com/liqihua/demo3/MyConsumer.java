package com.liqihua.demo3;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MyConsumer {

    public static void main(String[] args) throws MQClientException {
        //DefaultMQPullConsumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("example_group_name");
        consumer.setNamesrvAddr("120.77.69.159:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("TopicTestjjjj", "TagA || TagC || TagD");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            AtomicLong consumeTimes = new AtomicLong(0);
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,ConsumeOrderlyContext context) {
                for(MessageExt msg : msgs){
                    this.consumeTimes.incrementAndGet();
                    long num = this.consumeTimes.get();
                    System.out.println("--- get msg start : "+new String(msg.getBody())+"  num:"+num);
                    long millis = 2000;
                    if(num < 5){
                        millis = 10000;
                    }
                    try {Thread.sleep(millis);} catch (InterruptedException e) {e.printStackTrace();}
                    System.out.println("--- get msg end : "+new String(msg.getBody())+"  num:"+num);
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
