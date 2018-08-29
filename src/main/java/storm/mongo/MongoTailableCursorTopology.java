package storm.mongo;

import com.mongodb.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.Date;
import java.util.List;

import static org.apache.storm.utils.Utils.tuple;

/**
 * 
 * Quick and dirty test. Parts of this should probably be turned into a proper
 * integration test.
 * 
 * @author Dan Beaulieu
 *
 */
public class MongoTailableCursorTopology {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		
		// 准备 mongo 环境
        Mongo mongo = new MongoClient(new ServerAddress("localhost", 27017));
        mongo.dropDatabase("mongo_storm_tailable_cursor");
        final BasicDBObject options = new BasicDBObject("capped", true);
        options.put("size", 10000);
        mongo.getDB("mongo_storm_tailable_cursor").createCollection("test", options);
        final DBCollection coll = mongo.getDB("mongo_storm_tailable_cursor").getCollection("test");

        //建立拓扑
		TopologyBuilder builder = new TopologyBuilder();
        MongoSpout spout = new MongoSpout("localhost", 27017, "mongo_storm_tailable_cursor", "test", new BasicDBObject()) {

			private static final long serialVersionUID = 1L;

			@Override
        	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        		
        		declarer.declare(new Fields("document"));	
        	}

			@Override
			public List<Object> dbObjectToStormTuple(DBObject document) {
				
				return tuple(document);
			}
        	
        };
        builder.setSpout("1", spout);

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());

        //mongo 写数据
        Runnable writer = new Runnable() {

			@Override
			public void run() {
				for (int i=0; i < 1000; i++) {
	                final BasicDBObject doc = new BasicDBObject("_id", i);
	                doc.put("ts", new Date());
	                coll.insert(doc);
	            }
			}
		};
		
		new Thread(writer).start();
		
        Utils.sleep(10000);
        cluster.shutdown();
        
        mongo.dropDatabase("mongo_storm_tailable_cursor");

	}

}
