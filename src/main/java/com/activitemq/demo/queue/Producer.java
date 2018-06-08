package com.activitemq.demo.queue;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * @author guochunyuan
 * @create on  2018-06-08 14:10
 */
public class Producer {
    private final ActiveMQConnectionFactory factory;
    private final Connection connection;
    private final Session session;
    private final MessageProducer producer;
    private final String brokerURL="failover:(tcp://localhost:61616)?Randomize=false";

    public Producer() throws JMSException {
        factory = new ActiveMQConnectionFactory("admin","admin123",brokerURL);
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(null);
    }

    public void sendMessage() throws JMSException {
        String[] jobs ={"111","222"};
        for(int i = 0; i < jobs.length; i++){
            String job = jobs[i];
            Destination destination = session.createQueue("JOBS." + job);
            Message message = session.createObjectMessage(i+"test");
            System.out.println("Sending: id: " + ((ObjectMessage)message).getObject() + " on queue: " + destination);
            producer.send(destination, message);
        }
    }

    public static void main(String[] args) throws JMSException {
        Producer producer = new Producer();
        for(int i = 0; i < 10; i++) {
            producer.sendMessage();
            System.out.println("Produced " + i + " job messages");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    private void close() {
        try {
            if (connection!=null)
                connection.close() ;
        } catch (JMSException e) {
            e.printStackTrace();
        }

    }

}
