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

    private static void output(long [] data){
        for (long l : data) {
            System.out.print(l+" ");
        }
        System.out.println();
    }

    public static void main(String args[]){
        TopologyBuilder builder = new TopologyBuilder();
        Metrics m = new Metrics(1000);
        RandomEmitSpouts res = new RandomEmitSpouts("RandomOutput", 1000, 10);
        m.register(res);
        int LEVEL = 3;
        ConstBolt [] cb = new ConstBolt[LEVEL];
        for (int i = 0; i < LEVEL; i++) {
            cb[i] = new ConstBolt("constBolt_"+i,10);
            m.register(cb[i]);
        }

        builder.setSpout("RandomOutput", res, 10).setNumTasks(10);
        builder.setBolt("constBolt_0", cb[0], 10).setNumTasks(10).shuffleGrouping("RandomOutput");
        for (int i = cb.length - 1; i > 0; i--) {
            builder.setBolt("constBolt_" + i, cb[i], 10).setNumTasks(10).shuffleGrouping("constBolt_" + (i-1));
        }


        LocalCluster ls = new LocalCluster();
        ls.submitTopology("SOL", new Config(), builder.createTopology());

        m.start();

    }

}
