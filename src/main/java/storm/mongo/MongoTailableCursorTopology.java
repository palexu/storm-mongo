package storm.mongo;

import com.mongodb.BasicDBObject;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Quick and dirty test. Parts of this should probably be turned into a proper
 * integration test.
 *
 * @author Dan Beaulieu
 */
public class MongoTailableCursorTopology {
    private final static String topoName = "testMongo";

    /**
     * @param args
     */
    public static void main(String... args) throws Exception {
        //建立拓扑
        TopologyBuilder builder = new TopologyBuilder();
        AbstractMongoSpout spout = new MongoSpout("localhost", 27017, "mongo_storm_tailable_cursor", "test",
                                                  new BasicDBObject());
        builder.setSpout("1", spout);

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());

        Utils.sleep(10000);
        cluster.shutdown();
    }
}
