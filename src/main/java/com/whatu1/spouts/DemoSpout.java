package com.whatu1.spouts;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class DemoSpout implements IRichSpout {

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;
	private static String[] words = { "Hadoop", "Storm", "Apache", "Linux", "Nginx", "Tomcat", "Spark" };

	@Override
	public void ack(Object arg0) {
		System.out.println("DemoSpout的 ack()方法执行了: " + arg0);
	}

	@Override
	public void activate() {
		System.out.println("DemoSpout的 activate()方法执行了");
	}

	@Override
	public void close() {
		System.out.println("DemoSpout的 close()方法执行了");
	}

	@Override
	public void deactivate() {
		System.out.println("DemoSpout的 deactivate()方法执行了");
	}

	@Override
	public void fail(Object arg0) {
		System.out.println("DemoSpout的 fail()方法执行了: " + arg0);
	}

	/**
	 * @Description 把Tuple發至下游
	 */
	@Override
	public void nextTuple() {
		System.out.println("DemoSpout的 nextTuple()方法执行了");
		String word = words[new Random().nextInt(words.length)];
		collector.emit(new Values(word));
	}

	/**
	 * @Description 數據源初始化
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map arg0, TopologyContext context, SpoutOutputCollector collector) {
		System.out.println("DemoSpout的 fail()方法执行了: " + arg0 + " , " + context + " , " + collector);
		this.collector = collector;
	}

	/**
	 * @Description 定義輸出字段
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println("DemoSpout的 declareOutputFields()方法执行了: " + declarer);
		declarer.declare(new Fields("randomword"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		System.out.println("DemoSpout的 getComponentConfiguration()方法执行了");
		return null;
	}

}
