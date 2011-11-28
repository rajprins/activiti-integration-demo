package nl.ciber.knowledgeblog.mule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.activiti.engine.RuntimeService;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import nl.ciber.common.AbstractTest;
import org.junit.Test;
import org.mule.api.MuleContext;
import org.mule.context.DefaultMuleContextFactory;
import org.mule.module.activiti.action.model.ProcessInstance;

public class MuleHelloTest extends AbstractTest {

	private Session session;
	private Connection connection;
	private MessageProducer producer;
	private MessageConsumer consumer;

	@Test
	public void testSend() throws Exception {

		// configure the message broker
		System.out.println("Setting up Message Broker");
		BrokerService broker = new BrokerService();
		broker.addConnector("tcp://localhost:61616");
		broker.start();

		System.out.println("Setting up ESB");
		MuleContext muleContext = new DefaultMuleContextFactory().createMuleContext("mule-application-context.xml");
		muleContext.start();

		initialize();

		// Create a messages
		MapMessage message = session.createMapMessage();
		message.setString("processDefinitionKey", "helloWorldMule");
		message.setString("var1", "hello");

		// Tell the producer to send the message
		System.out.println("Sending message: " + message.hashCode() + " : " + Thread.currentThread().getName());
		producer.send(message);

		//Consume response message
		ObjectMessage responseMessage = (ObjectMessage) consumer.receive(2000);
		ProcessInstance processInstance = (ProcessInstance) responseMessage	.getObject();
		assertFalse(processInstance.isEnded());
		RuntimeService runtimeService = (RuntimeService) muleContext.getRegistry().get("runtimeService");
		Object result = runtimeService.getVariable(processInstance.getId(),	"var2");
		
		System.out.println("Response: " +result);		
		assertEquals("world", result);

		// Clean up
		session.close();
		connection.close();

		muleContext.stop();
		muleContext.dispose();
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
