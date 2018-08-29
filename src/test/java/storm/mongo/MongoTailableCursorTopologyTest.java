package storm.mongo;

import com.mongodb.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

public class MongoTailableCursorTopologyTest {
    Mongo mongo;
    DBCollection coll;

    @Before
    public void before() throws Exception {
        // 准备 mongo 环境
        mongo = new MongoClient(new ServerAddress("localhost", 27017));
        mongo.dropDatabase("mongo_storm_tailable_cursor");
        BasicDBObject options = new BasicDBObject("capped", true);
        options.put("size", 10000);
        mongo.getDB("mongo_storm_tailable_cursor").createCollection("test", options);
        coll = mongo.getDB("mongo_storm_tailable_cursor").getCollection("test");
    }

    @After
    public void after() {
        mongo.dropDatabase("mongo_storm_tailable_cursor");
    }

    public void mockData() {
        //mongo 写数据
        Runnable writer = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    final BasicDBObject doc = new BasicDBObject("_id", i);
                    doc.put("ts", new Date());
                    coll.insert(doc);
                }
            }
        };
        new Thread(writer).start();
    }

    @Test
    public void testMain() throws Exception {
        mockData();
        MongoTailableCursorTopology.main();
    }
}