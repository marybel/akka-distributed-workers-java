package messaging;

import akka.actor.Address;

public interface Callback {
	Address methodToCallback(Object message);
}
