package worker;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import java.util.Random;

public class WorkExecutor extends UntypedActor {
	private final boolean flakyWorkExecutor;
	private int executionCount = 0;
	private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
	private Random random = new Random();
	private String responsibilityLevel;

	public WorkExecutor() {
		this.flakyWorkExecutor = random.nextBoolean();
		responsibilityLevel = flakyWorkExecutor ? "F[" : "[N";
	}

	public WorkExecutor(boolean flakyWorkExecutor) {
		this.flakyWorkExecutor = flakyWorkExecutor;
		responsibilityLevel = flakyWorkExecutor ? "F[" : "[N";
	}

	@Override
	public void onReceive(Object message) {
		executionCount++;
		if (flakyWorkExecutor && random.nextBoolean()) {
			RuntimeException exception = random.nextBoolean() ? //
			new WorkExecutionException(responsibilityLevel + executionCount + "] OMG, not again!")//
					: new RuntimeException(responsibilityLevel + executionCount + "] Error :| ");
			throw exception;
		}
		if (message instanceof Integer) {
			String result = getResult((Integer) message);
			getSender().tell(new Worker.WorkComplete(result), getSelf());
		}
	}

	private String getResult(Integer message) {
		Integer n = message;
		int n2 = n.intValue() * n.intValue();

		String result = responsibilityLevel + executionCount + "]" + n + " * " + n + " = " + n2;
		log.debug("Produced result {}", result);
		return result;
	}
}
