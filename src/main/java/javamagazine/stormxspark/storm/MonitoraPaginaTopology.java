package javamagazine.stormxspark.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class MonitoraPaginaTopology {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("htmlSpout", new HtmlSpout(), 5);
		builder.setBolt("recuperaHtmlBolt", new RecuperaHtmlBolt(), 8).shuffleGrouping("spout");
		builder.setBolt("analisaHtmlBolt", new AnalisaHtmlBolt(), 12).fieldsGrouping("split", new Fields("word"));
		builder.setBolt("indexaHtmlBolt", new IndexaHtmlBolt(), 8).shuffleGrouping("spout");
		
		Config conf = new Config();
		conf.setDebug(true);
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar("JMTopology", conf,
					builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("word-count", conf, builder.createTopology());
			Thread.sleep(10000);
			cluster.shutdown();
		}
	}

}
