package com.liqihua.demo3;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;


public class MyOrderedProducer {
    static int num = 0;


    public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException, UnsupportedEncodingException {
        DefaultMQProducer producer = new DefaultMQProducer("example_group_name");
        producer.setDefaultTopicQueueNums(1);
        producer.setNamesrvAddr("120.77.69.159:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message msg = new Message("TopicTestjjjj", "TagA", "KEY" + i,("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    System.out.println("size:"+mqs.size());
                    for(MessageQueue queue : mqs){
                        System.out.print(queue.getQueueId()+" | ");
                    }
                    int index = 0;
                    if(num % 2 == 0){
                        index = 1;
                    }else{
                        index = 0;
                    }
                    num += 1;
                    return mqs.get(index);
                }
            }, i);
            System.out.println("--- send : "+new String(msg.getBody())+",tag:"+msg.getTags()+",result:"+sendResult.getSendStatus());
        }
        producer.shutdown();
    }



}
