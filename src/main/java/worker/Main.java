package worker;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.Props;
import akka.cluster.Cluster;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import messaging.Callback;
import messaging.ListeningActor;
import messaging.RabbitMqConnection;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

public class Main {
	private static final boolean DURABLE = true;
	private static final boolean NON_EXCLUSIVE = false;
	private static final boolean NON_AUTO_DELETE = false;
	public static final FiniteDuration SCHEDULE_DELAY = Duration.create(2, "seconds");
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

	private static void setupListener(Channel channel, String queueName, String exchange, Callback callback)
			throws IOException {
		channel.queueBind(queueName, exchange, "");

		ActorSystem system = ActorSystem.create(systemName);
		Address joinAddress = (new SystemStarter()).methodToCallback(systemName);
		Cluster.get(system).join(joinAddress);
		ActorRef listeningActor = system.actorOf(Props.create(ListeningActor.class, channel, queueName, callback),
				"listeningActor");
		system.scheduler().scheduleOnce(SCHEDULE_DELAY, listeningActor, "", system.dispatcher(),
				null);
	}
}
