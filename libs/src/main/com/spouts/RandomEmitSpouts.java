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
import com.metrics.MetricComponent;

import java.util.Map;
import java.util.Random;

public class RandomEmitSpouts extends BaseRichSpout {

    private int EMIT_SIZE;
    private static final int _DEFAULT_EMIT_SIZE = 100;

    private Random _r;
    private SpoutOutputCollector _collector;
    private int _localIndex;
    private int _componentId;

    public RandomEmitSpouts(String name, int size, int taskNum){
        EMIT_SIZE = size;
        _componentId = MetricComponent.register(name,taskNum);
    }
    public RandomEmitSpouts(String name, int taskNum){
        this(name, _DEFAULT_EMIT_SIZE, taskNum);
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        _r = new Random(System.currentTimeMillis());
        _localIndex = topologyContext.getThisTaskIndex();
    }

    public void nextTuple() {
        byte [] data = new byte[EMIT_SIZE];
        _r.nextBytes(data);
        MetricComponent.tick(_componentId, _localIndex, data.length);
        _collector.emit(new Values(data));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("object"));
    }

    public long getEmitSize(){return EMIT_SIZE;}

}
