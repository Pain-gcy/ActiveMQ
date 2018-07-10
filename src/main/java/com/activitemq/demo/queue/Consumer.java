package com.activitemq.demo.queue;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * @author guochunyuan
 * @create on  2018-06-08 14:40
 */
public class Consumer {
    private final ActiveMQConnectionFactory factory;
    private final Connection connection;
    private final Session session;
    private final String brokerURL="failover:(tcp://localhost:61616)?Randomize=false";
    private String[] jobs = {"111","222"};

    public Consumer() throws Exception {
        factory = new ActiveMQConnectionFactory("admin","admin123",brokerURL);
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }
    public Session getSession() {
        return session;
    }
    public static void main(String[] args) throws Exception {
        Consumer consumer = new Consumer();
        for (String job : consumer.jobs) {
            Destination destination = consumer.getSession().createQueue("JOBS." + job);
            MessageConsumer messageConsumer = consumer.getSession().createConsumer(destination);
            //添加监听；
            messageConsumer.setMessageListener(new Listener(job));
        }
    }
}
