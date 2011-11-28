package nl.ciber.knowledgeblog.camel;

import static org.junit.Assert.assertEquals;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.activiti.engine.RuntimeService;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import nl.ciber.common.AbstractTest;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class CamelHelloTest extends AbstractTest {

	private Session session;
	private Connection connection;
	private MessageProducer producer;
	private MessageConsumer consumer;

	@Test
	public void simpleProcessTest() throws Exception {

		// configure the broker		
		BrokerService broker = new BrokerService();
		broker.addConnector("tcp://localhost:61616");
		broker.start();

		ApplicationContext ctx = new FileSystemXmlApplicationContext("classpath:application-context.xml");

		initialize();

		// Create a messages
		MapMessage message = session.createMapMessage();
		message.setString("var1", "hello");

		// Tell the producer to send the message
		System.out.println("Sending message: " + message.hashCode() + " : " + Thread.currentThread().getName());
		producer.send(message);

		System.out.println("Consuming message(s)");
		TextMessage responseMessage = (TextMessage) consumer.receive(4000);
		System.out.println(responseMessage);
		String instanceId = responseMessage.getText();

		assertEquals("world", ctx.getBean("runtimeService", RuntimeService.class).getVariable(instanceId, "var2"));

		session.close();
		connection.close();
		broker.stop();
	}

	private void initialize() throws Exception {
		// Create a ConnectionFactory
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

		// Create a Connection
		connection = connectionFactory.createConnection();
		connection.start();

		// Create a Session
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		// Create the destination (Topic or Queue)
		Destination destination = session.createQueue("in.create");

		// Create a MessageProducer from the Session to the Topic or Queue
		producer = session.createProducer(destination);

		// Create the destination (Topic or Queue)
		Destination responseDestination = session.createQueue("out.create");
		consumer = session.createConsumer(responseDestination);
	}
}
