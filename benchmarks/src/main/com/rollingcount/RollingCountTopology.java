package com.rollingcount; /**
 * Created by tao on 27/07/15.
 */
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import backtype.storm.tuple.Fields;
import com.metrics.Metrics;
import com.metrics.RollingCountBolt;
import com.spouts.TwitterTrendSpout;

public class RollingCountTopology {


    public static void main(String args[]){
        int SPOUT_NUM = 1;
        int BOLT_NUM = 5;
        TopologyBuilder builder = new TopologyBuilder();
        Metrics m = new Metrics(1000);


        TwitterTrendSpout res = new TwitterTrendSpout("RandomOutput", SPOUT_NUM);
        RollingCountBolt rcb = new RollingCountBolt("rollingcount", BOLT_NUM);


        builder.setSpout("RandomOutput", res, SPOUT_NUM);
        builder.setBolt("rolling_count", rcb, BOLT_NUM).setNumTasks(BOLT_NUM).fieldsGrouping("RandomOutput", new Fields("word"));


        LocalCluster ls = new LocalCluster();

        ls.submitTopology("rollingcount", new Config(), builder.createTopology());

        m.start();

    }

}
