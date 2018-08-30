package storm.mongo.novel;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.Map;

import static org.apache.storm.utils.Utils.tuple;

public class NovelMongoSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    private DB mongoDB;
    private final DBObject query;

    private final String mongoHost;
    private final int mongoPort;
    private final String mongoDbName;
    private final String mongoCollectionName;

    public NovelMongoSpout(String mongoHost, int mongoPort, String mongoDbName, String mongoCollectionName,
                           DBObject query) {
        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        this.mongoDbName = mongoDbName;
        this.mongoCollectionName = mongoCollectionName;
        this.query = query;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.mongoDB = new MongoClient(this.mongoHost, this.mongoPort).getDB(this.mongoDbName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        DBObject dbo = this.mongoDB.getCollection(mongoCollectionName).findOne();
        if (dbo == null) {
            Utils.sleep(50);
        } else {
            this.collector.emit(tuple(dbo));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("novel"));
    }
}
