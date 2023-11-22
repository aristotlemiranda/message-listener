package sg.com.nets.batch.service;

import javax.jms.*;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.springframework.boot.autoconfigure.jms.JmsProperties.AcknowledgeMode;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import sg.com.nets.batch.listener.RetrieveMessageListener;

@Component
public class MessagingService {

	public JMSContext context;
	@Bean
	public JmsConnectionFactory connectionFactory() throws Exception  {
		//to control redelivery in jndi level set jms.redeliveryPolicy.maxRedeliveries=5, NOTE: this option will stop the consumer binding when it reached the threshold
		JmsConnectionFactory connectionFactory = new JmsConnectionFactory("admin", "admin", "amqp://localhost:5672");
		connectionFactory.setClientID("TWTAMM-2424");

		Connection connection = connectionFactory.createConnection();
		Session session = connection.createSession(false, AcknowledgeMode.AUTO.getMode());
		context = connectionFactory.createContext(Session.CLIENT_ACKNOWLEDGE);
		Queue q = context.createQueue("test.message");
		JMSConsumer consumer = context.createConsumer(q);
		consumer.setMessageListener(new RetrieveMessageListener(context));
		connection.start();
		return connectionFactory;
	}


}
