package com.metrics;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.metrics.IMetric;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by tao on 27/07/15.
 */
public class MetricComponent{

    private static ArrayList<ArrayList<Long>> _GCounts = new ArrayList<ArrayList<Long>>();
    private static ArrayList<ArrayList<Long>> _GBytes = new ArrayList<ArrayList<Long>>();
    private static ArrayList<String> _registedNames = new ArrayList<String>();

    public MetricComponent(){
    }
    private static ArrayList<Long> initData(int size){
        ArrayList<Long> ret = new ArrayList<Long>(size);
        for (int i = 0; i < size; i++) {
            ret.add(0L);
        }
        return ret;
    }

    public static int register(String name, int taskNum){
        _registedNames.add(name);
        _GCounts.add(initData(taskNum));
        _GBytes.add(initData(taskNum));
        Metrics.register(taskNum);
        return _registedNames.size()-1;//index
    }

    public static void tick(int componentId, int index, int size){
        long count = _GCounts.get(componentId).get(index)+1;
        _GCounts.get(componentId).set(index, count);
        count = _GBytes.get(componentId).get(index) + size;
        _GBytes.get(componentId).set(index, count);
    }


    public static ArrayList<ArrayList<Long>> getTotalCount() {
        return _GCounts;
    }


    public static ArrayList<String> getName() {
        return _registedNames;
    }


    public static ArrayList<ArrayList<Long>> getTotalBytes() {
        return _GBytes;
    }
}
