package com.spouts;
/**
 * Created by tao on 27/07/15.
 * random emit spouts will emit random bytes as specified.
 */

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.metrics.IMetric;

import java.util.Map;
import java.util.Random;

public class RandomEmitSpouts extends BaseRichSpout implements IMetric {

    private int EMIT_SIZE;
    private static final int _DEFAULT_EMIT_SIZE = 100;
    private int NUM_TASKS;


    private static long [] _totalCount;
    private String _name;

    private Random _r;
    private SpoutOutputCollector _collector;
    private int _currentIndex;

    public RandomEmitSpouts(String name, int size, int taskNum){
        EMIT_SIZE = size;
        NUM_TASKS = taskNum;
        _totalCount = new long[NUM_TASKS];
        _name = name;
    }
    public RandomEmitSpouts(String name, int taskNum){
        this(name, _DEFAULT_EMIT_SIZE, taskNum);
    }


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        _currentIndex = topologyContext.getThisTaskIndex();
        _r = new Random(System.currentTimeMillis());
        _totalCount[_currentIndex] = 0;
    }

    public void nextTuple() {
        _totalCount[_currentIndex] ++;
        byte [] data = new byte[EMIT_SIZE];
        _r.nextBytes(data);
        _collector.emit(new Values(data));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("object"));
    }

    public long getEmitSize(){return EMIT_SIZE;}

    @Override
    public long[] getTotalCount() {
        return _totalCount;
    }

    @Override
    public long[] getTotalBytes() {
        long [] data = new long [NUM_TASKS];
        for (int i = 0; i < data.length; i++) {
            data[i] = _totalCount[i] * EMIT_SIZE;
        }
        return data;
    }

    @Override
    public String getName() {
        return _name;
    }
}
