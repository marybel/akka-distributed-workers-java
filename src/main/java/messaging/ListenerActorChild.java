package messaging;

import akka.actor.UntypedActor;

public class ListenerActorChild extends UntypedActor {
	private final Callback callback;

	public ListenerActorChild(Callback callback) {
		this.callback = callback;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message != null) {
			callback.methodToCallback(message);
		}
	}
}