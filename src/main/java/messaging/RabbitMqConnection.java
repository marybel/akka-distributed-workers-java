package messaging;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;

public class RabbitMqConnection {

	private static Connection connection = null;


	// Return a connection if one doesn't exist. Else create a new one
	public static Connection getConnection() {
		if (connection == null) {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(Config.RABBITMQ_HOST);
			try {
				return factory.newConnection();
			} catch (IOException e) {
				System.out.println("RabbitMQ connection not acquired"+ e.getMessage());
			}
		}

		return connection;
	}
}
