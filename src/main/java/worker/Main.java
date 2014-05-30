package worker;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import messaging.Callback;
import messaging.ListeningActor;
import messaging.RabbitMqConnection;
import scala.concurrent.duration.Duration;

public class Main {
	private static final boolean DURABLE = true;
	private static final boolean NON_EXCLUSIVE = false;
	private static final boolean NON_AUTO_DELETE = false;
	private static String systemName = "Workers";

	public static void main(String[] args) throws Exception {
		// create the connection
		Connection connection = RabbitMqConnection.getConnection();
		// create a channel for the listener and setup the first listener
		Channel listenChannel1 = connection.createChannel();
		AMQP.Queue.DeclareOk declareOk = listenChannel1.queueDeclare("", DURABLE, NON_EXCLUSIVE, NON_AUTO_DELETE, null);
		setupListener(listenChannel1, declareOk.getQueue(),
				messaging.Config.RABBITMQ_EXCHANGE, new SystemStarter());
	}

	private static void setupListener(Channel channel, String queueName, String exchange, Callback f)
			throws IOException {
		channel.queueBind(queueName, exchange, "");
		ActorSystem system = ActorSystem.create(systemName);
		system.scheduler().scheduleOnce(Duration.create(2, "seconds"),
				system.actorOf(Props.create(ListeningActor.class, channel, queueName, f),"listeningActor"), "", system.dispatcher(),
				null);
	}
}
