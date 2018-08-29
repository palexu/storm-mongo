package storm.mongo;

import com.mongodb.DBObject;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.List;

import static org.apache.storm.utils.Utils.tuple;

public class MongoSpout extends AbstractMongoSpout {
    private static final long serialVersionUID = 1L;

    public MongoSpout(String mongoHost, int mongoPort, String mongoDbName, String mongoCollectionName, DBObject query) {
        super(mongoHost, mongoPort, mongoDbName, mongoCollectionName, query);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("document"));
    }

    @Override
    public List<Object> dbObjectToStormTuple(DBObject document) {
        return tuple(document);
    }
}
