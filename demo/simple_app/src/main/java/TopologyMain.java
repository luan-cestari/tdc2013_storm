import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader",new WordReaderSpout());
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
