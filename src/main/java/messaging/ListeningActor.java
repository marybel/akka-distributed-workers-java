package messaging;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import worker.Worker;

public class ListeningActor extends UntypedActor {
	private static final boolean AUTO_ACK = true;
	private static final boolean EXPLICIT_ACK = false;
	public static final boolean REQUEUE = true;
	public static final boolean SINGLE = false;
	private final Channel channel;
	private final String queueName;
	private final Callback callback;
	private static String systemName = "Workers";
	private boolean workOngoing;
	private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	public ListeningActor(Channel channel, String queueName, Callback callback) {
		this.channel = channel;
		this.queueName = queueName;
		this.callback = callback;
	}

	public void startReceving() {
		try {
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queueName, EXPLICIT_ACK, consumer);

			while (true) {
				relayMessageToChild(consumer);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	private void relayMessageToChild(QueueingConsumer consumer) throws Exception {
		// wait for the message
		QueueingConsumer.Delivery delivery = consumer.nextDelivery();
		long deliveryTag = delivery.getEnvelope().getDeliveryTag();
		if (workOngoing) {
			channel.basicNack(deliveryTag, SINGLE, REQUEUE);
		} else {
			String msg = new String(delivery.getBody());

			// send the message to the provided callback function and
			// execute
			// this in a subactor
			ActorSystem system = ActorSystem.create(systemName);
			ActorRef listenerActorChild = system.actorOf(Props.create(ListenerActorChild.class, callback));
			listenerActorChild.tell(msg, null);
			channel.basicAck(deliveryTag, SINGLE);
			workOngoing = true;
		}

	}

	@Override
	public void onReceive(Object message) throws Exception {
		log.info("ListeningActor path: " + getSelf().path().toStringWithoutAddress());
		if (message instanceof Worker.WorkComplete) {
			workOngoing = false;
			log.info("workOngoing = " + workOngoing);
		} else
			startReceving();
	}
}
