package storm.mongo.novel;

import com.google.common.base.Splitter;
import com.mongodb.BasicDBObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;

public class ContentBolt extends BaseRichBolt {
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        BasicDBObject novel = (BasicDBObject) input.getValueByField("novel");
        String rawContent = novel.getString("content");
        novel.put("raw_content", rawContent);

        //clean
        String content = rawContent.replace("\\n", "").replace("\\t", "");
        List<String> list = Splitter.on("\n").trimResults().omitEmptyStrings().splitToList(content);

        boolean start = false;
        boolean end = false;
        boolean infoParsed = false;

        StringBuilder builder = new StringBuilder();
        for (String line : list) {
            if (!start && line.contains("---|---|---")) {
                start = true;
                continue;
            }
            if (line.contains("小强文学网-http://www.335xs.com")) {
                end = true;
                break;
            }

            if (start && !end) {
                builder.append(line);
                if (line.endsWith("。")) {
                    builder.append("\n");
                    builder.append("  ");
                }
            }

            if (!infoParsed && line.contains("作者") && line.contains("类别")) {
                infoParsed = true;
                List<String> infos = Splitter.on(" ").splitToList(line);
                infos.forEach(info -> {
                    if (info.contains("作者")) {
                        novel.put("author", info.replace("作者：", ""));
                    }
                    if (info.contains("类别")) {
                        novel.put("category", info.replace("类别：", ""));
                    }
                    if (info.contains("本章")) {
                        novel.put("subtitle", info.replace("本章：", ""));
                    }
                });
            }
        }
        content = builder.toString();
        novel.put("content", content);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
