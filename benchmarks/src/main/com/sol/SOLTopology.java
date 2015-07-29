package com.sol; /**
 * Created by tao on 27/07/15.
 */
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.bolts.ConstBolt;
import com.metrics.Metrics;
import com.spouts.RandomEmitSpouts;

public class SOLTopology {


    public static void main(String args[]){
        int SPOUT_NUM = 3;
        int BOLT_NUM = 4;
        int LEVEL = 1;
        TopologyBuilder builder = new TopologyBuilder();
        Metrics m = new Metrics(1000);
        RandomEmitSpouts res = new RandomEmitSpouts("RandomOutput", 4, SPOUT_NUM);
        ConstBolt [] cb = new ConstBolt[LEVEL];
        for (int i = 0; i < LEVEL; i++) {
            cb[i] = new ConstBolt("constBolt_"+i,BOLT_NUM);
        }

        builder.setSpout("RandomOutput", res, SPOUT_NUM);
        builder.setBolt("constBolt_0", cb[0], BOLT_NUM).setNumTasks(BOLT_NUM).shuffleGrouping("RandomOutput");
        for (int i = cb.length - 1; i > 0; i--) {
            builder.setBolt("constBolt_" + i, cb[i], BOLT_NUM).setNumTasks(BOLT_NUM).shuffleGrouping("constBolt_" + (i-1));
        }


        LocalCluster ls = new LocalCluster();

        ls.submitTopology("SOL", new Config(), builder.createTopology());

        m.start();

    }

}
