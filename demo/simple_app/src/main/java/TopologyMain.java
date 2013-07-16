import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.common.collect.ImmutableList;
import storm.kafka.SpoutConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.KafkaConfig;
import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.KafkaConfig.ZkHosts;
import java.util.ArrayList;
import java.util.List;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();
		//List<String> l = new ArrayList<String>();
		//l.add("localhost:");
		//l.add("localhost:59963");
		//l.add("localhost:35256");
		//l.add("localhost:9092");
		//StaticHosts m = null;
		//m=StaticHosts.fromHostString(
		//		l, // list of Kafka brokers
		//		8 // number of partitions per host
		//);
		ZkHosts m = new ZkHosts("localhost:9092","/");
		SpoutConfig spoutConfig = new SpoutConfig(
			m,
			"test", // topic to read from
			"/kafkastorm", // the root path in Zookeeper for the spout to store the consumer offsets
			"discovery"); // an id for this consumer for storing the consumer offsets in Zookeeper
			builder.setSpout("word-reader",new KafkaSpout(spoutConfig));
		builder.setBolt("word-normalizer", new WordNormalizerBolt())
			.shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounterBolt(),1)
			.fieldsGrouping("word-normalizer", new Fields("word"));
		
		Config conf = new Config();
		conf.put("test_file", args[0]);
		conf.setDebug(false);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("simple_app", conf, builder.createTopology());
		Thread.sleep(3000);
		cluster.shutdown();
	}
}
