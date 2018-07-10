package com.activitemq.demo.topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQMapMessage;

import javax.jms.*;

/**
 * @author guochunyuan
 * @create on  2018-06-11 9:14
 */
public class Publisher {
    private final ActiveMQConnectionFactory factory;
    private final Connection connection;
    private final Session session;
    private final MessageProducer producer;
    private final String brokerURL="failover:(tcp://localhost:61616)?Randomize=false";
    private Destination[] destinations;

    //初始化 发布者
    public Publisher() throws JMSException {
        factory = new ActiveMQConnectionFactory("admin","admin123",brokerURL);
        connection = factory.createConnection();
        try {
            connection.start();
        } catch (JMSException jmse) {
            connection.close();
            throw jmse;
        }
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(null);
    }

    //设置 主题对话
    protected void setTopics(String[] stocks) throws JMSException {
        destinations = new Destination[stocks.length];
        for(int i = 0; i < stocks.length; i++) {
            destinations[i] = session.createTopic("STOCKS." + stocks[i]);
        }
    }
    //给每个对哈发消息；
    protected void sendMessage(String[] stocks) throws JMSException {
        for(int i = 0; i < stocks.length; i++) {
            Message message = createStockMessage(stocks[i], session);
            System.out.println("Sending: " + ((ActiveMQMapMessage)message).getContentMap() + " on destination: " + destinations[i]);
            producer.send(destinations[i], message);
        }
    }

    // 创建每个对话的 消息
    protected Message createStockMessage(String stock, Session session) throws JMSException {
        MapMessage message = session.createMapMessage();
        message.setString("stock", stock);
        message.setDouble("price", 1.00);
        message.setDouble("offer", 0.01);
        message.setBoolean("up", true);
        return message;
    }

    public static void main(String[] args) throws JMSException {
        String [] argss = {"1","2"};
        if(argss.length < 1)
            throw new IllegalArgumentException();

        // Create publisher
        Publisher publisher = new Publisher();

        // Set topics
        publisher.setTopics(argss);

        for(int i = 0; i < 10; i++) {
            publisher.sendMessage(argss);
            System.out.println("Publisher '" + i + " price messages");
            try {
                Thread.sleep(1000);
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
        // Close all resources
        publisher.close();
    }

    public void close() throws JMSException {
        if (connection != null) {
            connection.close();
        }
    }
}
