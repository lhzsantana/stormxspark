package javamagazine.stormxspark.storm;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@SuppressWarnings("serial")

public class AnalisaHtmlBolt extends BaseBasicBolt {

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String link = ((Element) tuple.getValue(0)).attr("abs:href");
		
		Document doc = Jsoup.parse(link);
try {
			XContentBuilder json = jsonBuilder().startObject();
			json.field("page").value(doc.text());
			json.field("date").value(new Date());
			json.field("category").value(doc.text());
			json.field("hash").value(json.hashCode());
			json.endObject();	
			collector.emit(new Values(json));
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("json"));
	}
	
	private String findCategory(String text){
		Map<String,Integer> words_count = new HashMap<String,Integer>();
		String[] words = text.split("\\s+");
		for(int i=0;i<words.length;i++) {
		     String s = words[i];
		     if(words_count.keySet().contains(s)) {
		          Integer count = words_count.get(s) + 1;
		          words_count.put(s, count);
		     } else{
		          words_count.put(s, 1);		    	 
		     }
		}
		Integer frequency = null;
		String mostFrequent = null;
		for(String s : words_count.keySet()) {
		    Integer i = words_count.get(s);
		    if(frequency == null)
		         frequency = i;
		    if(i > frequency) {
		         frequency = i;
		         mostFrequent = s;
		    }
		}
		return mostFrequent;
	}
}