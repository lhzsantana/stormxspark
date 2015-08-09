package javamagazine.stormxspark.spark;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class RecuperaHtml {
	
	private final String page = "www.globo.com";
	
	public void main(String[] args) {
		
		Document doc = Jsoup.parse(page);
		try {
			Socket socket = new Socket("localhost", 9999);
			if(checkHashCode(doc.location())==doc.hashCode()){
				Elements links = doc.select("a[href*=event-details]");
				OutputStream outstream = socket.getOutputStream(); 
				PrintWriter out = new PrintWriter(outstream);
			    for(Element link: links){
					out.print(link.attr("abs:href"));
				}
			}	
			socket.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
 
	private int checkHashCode(String url){
		return 0;
	}
}