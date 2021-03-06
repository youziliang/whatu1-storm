package com.whatu1.bolts;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class DemoBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		System.out.println("DemoBolt的 prepare()方法执行了: " + stormConf + " , " + context + " , " + collector);
	}

	@Override
	public void execute(Tuple input) {
		System.out.println("DemoBolt的 execute()方法执行了");
		String word = (String) input.getValue(0);
		String out = "Hello " + word + "!";
		System.out.println(out);
	}

	@Override
	public void cleanup() {
		System.out.println("DemoBolt的 cleanup()方法执行了");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println("DemoBolt的 declareOutputFields()方法执行了: " + declarer);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		System.out.println("DemoBolt的 getComponentConfiguration()方法执行了");
		return null;
	}

}
