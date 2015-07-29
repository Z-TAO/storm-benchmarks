package com.union; /**
 * Created by tao on 27/07/15.
 */
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import backtype.storm.tuple.Fields;
import com.bolts.RationalUnion;
import com.metrics.Metrics;
import com.bolts.RollingCountBolt;
import com.spouts.TwitterTrendSpout;
import com.spouts.UnionSpout;

public class RUTopology {


    public static void main(String args[]) throws Exception{
        int SPOUT_NUM = 2;
        int BOLT_NUM = 10;
        TopologyBuilder builder = new TopologyBuilder();
        Metrics m = new Metrics(1000);


        UnionSpout res = new UnionSpout("UnionSpout", SPOUT_NUM);
        RationalUnion rcb = new RationalUnion("RationalUnion", BOLT_NUM);


        builder.setSpout("UnionSpout", res, SPOUT_NUM);
        builder.setBolt("RationalUnion", rcb, BOLT_NUM).setNumTasks(BOLT_NUM).fieldsGrouping("UnionSpout", new Fields("id"));


        LocalCluster ls = new LocalCluster();

        ls.submitTopology("Union", new Config(), builder.createTopology());

        m.start();

    }

}
