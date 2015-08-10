package com; /**
 * Created by tao on 27/07/15.
 */
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.bolts.ConstBolt;
import com.bolts.SplitScentenceBolt;
import com.bolts.WordCountBolt;
import com.metrics.Metrics;
import com.spouts.RandomEmitSpouts;
import com.spouts.SplitScentenceSpout;
import sun.security.krb5.internal.LocalSeqNumber;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class WordCountTopology {


    public static void main(String args[]) {


    }

}
