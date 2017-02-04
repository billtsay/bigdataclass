package nyu.cs9223;

import java.io.IOException;
import java.util.Properties;
import java.util.TimerTask;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class StockInput {
	static final String QUOTE_TOPIC = "stockquote";
	static final String ERROR_TOPIC = "errorquote";

	public static void main(String[] args) {
		java.util.Timer t = new java.util.Timer();
			t.schedule(new TimerTask() {
				@Override
				public void run() {
					publish(args);
				}
			}, 5000, 10000);
	}

	// get stock quotes from a list of stock symbols by accessing google finance.
	// the quotes data are in json form, which is good for us to publish messages to kafka topic.
	public static void publish(String[] quotes) {
		Properties properties = new Properties();
		properties.put("metadata.broker.list", "localhost:9092");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig producerConfig = new ProducerConfig(properties);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				producerConfig);

		for (String q : quotes) {
			try {
				String msg = getJsonQuote(q);
				KeyedMessage<String, String> message = new KeyedMessage<String, String>(QUOTE_TOPIC, msg);
				producer.send(message);
			} catch (IOException e) {
				KeyedMessage<String, String> error = new KeyedMessage<String, String>(ERROR_TOPIC,
						q.concat(":".concat(e.getMessage())));
				producer.send(error);
			}
		}

		producer.close();
	}

	public static String getJsonQuote(String stock) throws IOException {
		String googleResponse = HttpUtil.get("http://finance.google.com/finance/info?client=ig&q=".concat(stock));
		String token[] = StringUtils.split(googleResponse, "//");
		String token1[] = StringUtils.split(token[1], "[");
		String token2[] = StringUtils.split(token1[1], "]");

		return token2[0];
	}

	public static StockQuote getQuote(String stock) throws IOException {
		String googleResponse = HttpUtil.get("http://finance.google.com/finance/info?client=ig&q=".concat(stock));
		String token[] = StringUtils.split(googleResponse, "//");

		Gson gson = new GsonBuilder().create();
		StockQuote[] quotes = gson.fromJson(token[1], StockQuote[].class);

		return quotes[0];
	}
}
