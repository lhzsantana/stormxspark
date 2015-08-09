package javamagazine.stormxspark.storm;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class IndexaHtmlBolt extends BaseBasicBolt {

	private static Client client;
	private static TransportClient transportClient;

	private final static String server = "xxx.xxx.xxx.xx";
	private final static String port = "9300";
	private final static String cluster = "elasticsearch";

	public static synchronized Client getClient() throws Exception {
		if (client == null) {
			Settings settings = ImmutableSettings.settingsBuilder()
					.put("cluster.name", cluster).build();
			transportClient = new TransportClient(settings);
			client = transportClient
					.addTransportAddress(new InetSocketTransportAddress(server,
							Integer.parseInt(port)));
		}
		return client;
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {

		XContentBuilder json = (XContentBuilder) tuple.getValue(0);
		try {
			getClient().prepareIndex().setSource(json).execute();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			client.close();
			transportClient.close();
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
	}

}