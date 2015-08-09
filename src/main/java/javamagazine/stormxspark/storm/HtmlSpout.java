package javamagazine.stormxspark.storm;

import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class HtmlSpout extends BaseRichSpout {

	private final String page="www.globo.com"; 
	SpoutOutputCollector _collector;

	public void nextTuple() {		
	    Document doc = Jsoup.parse(page);
	    _collector.emit(new Values(doc));
	}
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("page"));
	}
	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

}
