package messaging;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.contrib.pattern.DistributedPubSubExtension;
import akka.contrib.pattern.DistributedPubSubMediator;
import akka.dispatch.Mapper;
import akka.dispatch.OnComplete;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import akka.dispatch.Recover;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.util.Timeout;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import worker.Master;

import static akka.pattern.Patterns.ask;

public class ListeningActor extends UntypedActor {
	public static final boolean NO_LOCAL_AFFINITY = false;
	public static final Timeout TIMEOUT_5_SECONDS = new Timeout(Duration.create(5, "seconds"));
	private static final boolean AUTO_ACK = true;
	private static final boolean EXPLICIT_ACK = false;
	public static final boolean REQUEUE = true;
	public static final boolean SINGLE = false;
	private final Channel channel;
	private final String queueName;
	private final Callback callback;
	private static String systemName = "Workers";
	private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	private ActorRef mediator = DistributedPubSubExtension.get(getContext().system()).mediator();
	private ExecutionContext execContext = getContext().system().dispatcher();
	private long deliveryTag;

	public ListeningActor(Channel channel, String queueName, Callback callback) {
		this.channel = channel;
		this.queueName = queueName;
		this.callback = callback;

	}

	public void onReceive(Object message) {
		log.info("receiving message: " + message);
		startReceving();
	}

	private void relayWork(Master.Work newWork) {
		Future<Object> mastersResponseToWork = askMediatorToSendNewWorkToMaster(newWork);
		Future<Object> responseToNewWork = getResponseToNewWork(mastersResponseToWork, execContext);

		reply(responseToNewWork);
	}

	private void reply(Future<Object> responseToNewWork) {

		responseToNewWork.onSuccess(new OnSuccess<Object>() {

			public void onSuccess(Object v1) {
				log.info("CALLING OnSuccess: " + v1);
			}

		}, execContext);
		responseToNewWork.onFailure(new OnFailure() {
			public void onFailure(Throwable failure) {
				log.info("CALLING OnFailure");
				nack();
			}
		}, execContext);

		responseToNewWork.onComplete(new OnComplete<Object>() {
			@Override
			public void onComplete(Throwable failure, Object success) {
				log.info("CALLING OnComplete: " + success);
				nack();
			}
		}, execContext);
	}

	private void nack() {
		try {
			channel.basicNack(deliveryTag, SINGLE, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Future<Object> askMediatorToSendNewWorkToMaster(Master.Work newWork) {
		return ask(mediator, new DistributedPubSubMediator.Send("/user/master/active", newWork, NO_LOCAL_AFFINITY),
				TIMEOUT_5_SECONDS);
	}

	private Future<Object> getResponseToNewWork(Future<Object> mastersResponseToNewWork, ExecutionContext execContext) {
		return mastersResponseToNewWork.map(new Mapper<Object, Object>() {
			@Override
			public Object apply(Object msg) {
				if (msg instanceof Master.Ack)
					return Ok.getInstance();
				else
					return super.apply(msg);
			}
		}, execContext).recover(new Recover<Object>() {
			@Override
			public Object recover(Throwable failure) throws Throwable {
				return NotOk.getInstance();
			}
		}, execContext);
	}

	public static final class Ok implements Serializable {
		private Ok() {}

		private static final Ok instance = new Ok();

		public static Ok getInstance() {
			return instance;
		}

		@Override
		public String toString() {
			return "Ok";
		}
	};

	public static final class NotOk implements Serializable {
		private NotOk() {}

		private static final NotOk instance = new NotOk();

		public static NotOk getInstance() {
			return instance;
		}

		@Override
		public String toString() {
			return "NotOk";
		}
	};

	public void startReceving() {
		try {
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queueName, EXPLICIT_ACK, consumer);

			while (true) {
				// wait for the message
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				deliveryTag = delivery.getEnvelope().getDeliveryTag();

				String msg = new String(delivery.getBody());

				// send the new work message to the frontend
				log.info("Produced work: {}", msg);
				Master.Work work = new Master.Work("" + deliveryTag, msg);
				relayWork(work);

			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	private String nextWorkId() {
		return UUID.randomUUID().toString();
	}
}
