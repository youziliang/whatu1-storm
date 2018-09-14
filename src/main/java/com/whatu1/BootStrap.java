package com.whatu1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import com.whatu1.bolts.DemoBolt;
import com.whatu1.spouts.DemoSpout;

public class BootStrap {
	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new DemoSpout());
		// shuffleGrouping表示随机分组
		builder.setBolt("boltOne", new DemoBolt()).shuffleGrouping("spout");
		builder.setBolt("boltTwo", new DemoBolt()).shuffleGrouping("boltOne");

		Config conf = new Config();
		// 开发环境设置为true
		conf.setDebug(true);
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
				e.printStackTrace();
			}
		} else {
			// 模拟本地集群
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("bootstrap", conf, builder.createTopology());
			Utils.sleep(30000);
			cluster.killTopology("bootstrap");
			cluster.shutdown();
		}
	}

}
