package sg.com.nets.batch.listener;

import javax.jms.*;

import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import sg.com.nets.batch.service.MessagingService;

public class RetrieveMessageListener implements MessageListener {

    JMSContext context;
    int recoverCount = 0;

    public RetrieveMessageListener(JMSContext context) {
        this.context = context;
    }
	
	@Override
	public void onMessage(Message message) {
			System.out.println("onMessage BEGIN >>>>");
		try {
            if (message instanceof TextMessage) {
                System.out.printf("TextMessage received: '%s'%n", ((TextMessage) message).getText());
                System.out.println("Acknowledging.... ");
                message.acknowledge();               
            } else {
                System.out.println("Message received.");
                if (message instanceof BytesMessage){
                    BytesMessage byteMessage = (BytesMessage) message;
                    byte[] byteData = new byte[(int) byteMessage.getBodyLength()];
                    byteMessage.readBytes(byteData);
                    String msg = new String(byteData);
                    System.out.println("msg -> " + msg);

                    if(msg.equals("RECOVERY_MODE_ON")) {
                        if(recoverCount <= 10) {
                            context.recover();
                            System.out.println("recovering..... counter=" + recoverCount);
                            recoverCount++;
                            Thread.sleep(1000);
                        }
                    }else if(msg.contains("ACK_")) {
                        System.out.println("Acknowledging message");
                        message.acknowledge();
                    }else {
                        System.out.println("Message not acknowledge.");
                    }


                }
            }
        } catch (Exception ex) {
            System.out.println("Error processing incoming message.");
            ex.printStackTrace();
        }
        System.out.println("onMessage END <<<<");
	}


}
