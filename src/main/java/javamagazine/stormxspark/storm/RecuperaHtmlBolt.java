package javamagazine.stormxspark.storm;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
@SuppressWarnings("serial")

public class RecuperaHtmlBolt extends BaseBasicBolt {

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		Document doc = (Document) tuple.getValue(0);
		if(checkHashCode(doc.location())==doc.hashCode()){
			Elements links = doc.select("a[href*=event-details]");
			for(Element link: links){
				collector.emit(new Values(link));
			}
		}
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("link"));
	}
	private int checkHashCode(String url){
		return 0;
	}

}