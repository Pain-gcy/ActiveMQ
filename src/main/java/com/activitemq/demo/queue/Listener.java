package com.activitemq.demo.queue;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;

/**
 * @author guochunyuan
 * @create on  2018-06-08 14:49
 */
public class Listener implements MessageListener {
    private String job;

    public Listener(String job) {
        this.job = job;
    }
    @Override
    public void onMessage(Message message) {
        try {
            //do something here
            System.out.println(job + " id:" + ((ObjectMessage)message).getObject());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
