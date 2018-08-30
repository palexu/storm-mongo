package storm.mongo.novel;

import com.mongodb.BasicDBObject;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class NovelTopology {
    private static final String HOST = "localhost";
    private static final int PORT = 27017;

    private static final String DB = "novel";
    private static final String COLLECTION = "hnovel";

    public static void main(String... args) {
        TopologyBuilder builder = new TopologyBuilder();

        //spout
        NovelMongoSpout spout = new NovelMongoSpout(HOST, PORT, DB, COLLECTION, new BasicDBObject());
        builder.setSpout("novelReader", spout);
        //bolt
        ContentBolt bolt = new ContentBolt();
        builder.setBolt("contentBolt",bolt).shuffleGrouping("novelReader");

        //config
        Config conf = new Config();
        conf.setDebug(false);

        //topology
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("novel", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.shutdown();
    }
}
