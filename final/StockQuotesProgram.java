

package nyu.cs9223;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import scala.Tuple2;

public final class StockQuotesProgram {

	private StockQuotesProgram() {
	}

	public static void main(String[] args) {
		if (args.length < 4) {
			System.err.println("Usage: StockQuotesProgram <zkQuorum> <group> <topics> <numThreads>");
			System.exit(1);
		}

		StreamingExamples.setStreamingLogLevels();

		SparkConf sparkConf = new SparkConf().setAppName("StockQuotesProgram");
		
		// 1. collect data for this period Durations.minutes(#)
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.minutes(2));

		int numThreads = Integer.parseInt(args[3]);

		// 2. connect to topics of kafka and collect data.
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		String[] topics = args[2].split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}

		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1],
				topicMap);

		// 3. parse and convert the json qutoes data into java object StockQuote for the convenience of calculation.
		JavaDStream<StockQuote> quotes = messages.map(new Function<Tuple2<String, String>, StockQuote>() {
			@Override
			public StockQuote call(Tuple2<String, String> tuple2) {
				Gson gson = new GsonBuilder().create();
				return gson.fromJson(tuple2._2(), StockQuote.class);
			}
		});

		// 4. sum up the stock price for each stock during this period.
		JavaPairDStream<String, Double> price = quotes.mapToPair(new PairFunction<StockQuote, String, Double>() {
			@Override
			public Tuple2<String, Double> call(StockQuote t) throws Exception {
				return new Tuple2<String, Double>(t.getSymbol(), t.getPrice());
			}
		}).reduceByKey(new Function2<Double, Double, Double>() {
			@Override
			public Double call(Double i1, Double i2) {
				return i1 + i2;
			}
		});

		// 5. sum up the count of quotes for each stock during the same period.
		JavaPairDStream<String, Integer> count = quotes.mapToPair(new PairFunction<StockQuote, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(StockQuote t) throws Exception {
				return new Tuple2<String, Integer>(t.getSymbol(), 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		// 6. join the two streams price and count for each stock.
		JavaPairDStream<String, Tuple2<Double, Integer>> quote = price.join(count);

		// 7. convert the average price and post current date into java object AvgQuotes.
		JavaDStream<AvgQuotes> qs = quote.map(new Function<Tuple2<String, Tuple2<Double, Integer>>, AvgQuotes>() {
			@Override
			public AvgQuotes call(Tuple2<String, Tuple2<Double, Integer>> v) throws Exception {
				Double p = v._2()._1() / v._2()._2();
				Date date = new Date(); // Right now
				return AvgQuotes.newInstance(v._1(), p, date);
			}
		});

		// 8. persist each java object AvgQuotes into cassandra table cs9223.stockquotes.
		qs.foreachRDD(new VoidFunction<JavaRDD<AvgQuotes>>() {
			@Override
			public void call(JavaRDD<AvgQuotes> rdd) throws Exception {
				javaFunctions(rdd).writerBuilder("cs9223", "stockquotes", mapToRow(AvgQuotes.class)).saveToCassandra();
			}
		});

		qs.print();
		jssc.start();
		jssc.awaitTermination();
	}
}
