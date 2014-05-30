package worker;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.contrib.pattern.DistributedPubSubExtension;
import akka.contrib.pattern.DistributedPubSubMediator.Send;
import akka.dispatch.Mapper;
import akka.dispatch.Recover;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.util.Timeout;
import java.io.Serializable;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import static akka.pattern.Patterns.ask;
import static akka.pattern.Patterns.pipe;

public class Frontend extends UntypedActor {
	public static final boolean NO_LOCAL_AFFINITY = false;
	public static final Timeout TIMEOUT_5_SECONDS = new Timeout(Duration.create(5, "seconds"));

	private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	private ActorRef mediator = DistributedPubSubExtension.get(getContext().system()).mediator();
	private ExecutionContext execContext = getContext().system().dispatcher();

	public void onReceive(Object message) {
		log.info("receiving message: " + message);
		if (message instanceof Master.Work) {
			relayWork((Master.Work) message);
		} else {
			String errorMsg = "Non Work related message received by Frontend";
			log.error(errorMsg);
			throw new RuntimeException(errorMsg);
		}
	}

	private void relayWork(Master.Work message) {
		Master.Work newWork = (Master.Work) message;
		Future<Object> mediatorResponseToWork = askMediatorToSendNewWorkToMaster(newWork);
		Future<Object> responseToNewWork = getResponseToNewWork(mediatorResponseToWork, execContext);

		pipe(responseToNewWork, execContext).to(getSender());
	}

	private Future<Object> askMediatorToSendNewWorkToMaster(Master.Work newWork) {
		return ask(mediator, new Send("/user/master/active", newWork, NO_LOCAL_AFFINITY),
				TIMEOUT_5_SECONDS);
	}

	private Future<Object> getResponseToNewWork(Future<Object> mediatorResponseToWork, ExecutionContext execContext) {
		return mediatorResponseToWork.map(new Mapper<Object, Object>() {
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
}
