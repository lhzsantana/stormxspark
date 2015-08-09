package javamagazine.stormxspark.spark;

import java.io.IOException;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class MonitoraPagina {
	private static Client client;
	private static TransportClient transportClient;
	private final static String server = "198.23.188.82";
	private final static String port = "9301";
	private final static String cluster = "elasticsearch";
	
	@SuppressWarnings("resource")
	public static synchronized Client getClient() throws Exception {

		if (client == null) {
			TransportClient transportClient;

			Settings settings = ImmutableSettings.settingsBuilder()
					.put("cluster.name", cluster).build();
			transportClient = new TransportClient(settings);
			client = transportClient
					.addTransportAddress(new InetSocketTransportAddress(server, Integer.parseInt(port)));
		}

		return client;
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("MonitoraPagina");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		JavaReceiverInputDStream<String> pages = jssc.socketTextStream("localhost", 9999);
		
		pages.map(new Function<String, String>(){

			public String call(String arg0) throws Exception {
				Document doc = Jsoup.parse(arg0);
				try {
					XContentBuilder json = jsonBuilder().startObject();
					json.field("page").value(doc.text());
					json.field("date").value(new Date());
					json.field("category").value(doc.text());
					json.field("hash").value(json.hashCode());
					json.endObject();

				} catch (IOException e) {
					e.printStackTrace();
				}
				
				return null;
			}
		});

		pages.reduce(new Function2<String, String, String>(){
			public String call(String arg0, String arg1) throws Exception {
				try {
					client.prepareIndex().setSource(arg0).execute();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					client.close();
					transportClient.close();
				}
				return null;
			}
		});
	}
}