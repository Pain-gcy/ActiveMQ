package com.activitemq.demo.topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * @author guochunyuan
 * @create on  2018-07-10 9:20
 */
public class Consumer {
    private final ActiveMQConnectionFactory factory;
    private final Connection connection;
    private final Session session;
    private final String brokerURL = "failover:(tcp://localhost:61616)?Randomize=false";

    //初始化 发布者
    public Consumer() throws JMSException {
        factory = new ActiveMQConnectionFactory("admin", "admin123", brokerURL);
        connection = factory.createConnection();
        try {
            connection.start();
        } catch (JMSException jmse) {
            connection.close();
            throw jmse;
        }
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public static void main(String[] args) throws JMSException {
        String [] argss = {"1","2"};
        Consumer consumer = new Consumer();
        for (String stock : argss) {
            Destination destination = consumer.getSession().createTopic("STOCKS." + stock);
            MessageConsumer messageConsumer = consumer.getSession().createConsumer(destination);
            messageConsumer.setMessageListener(new MyListener());
        }
    }

    public Session getSession() {
        return session;
    }
}
