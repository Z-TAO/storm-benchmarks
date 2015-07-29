package com.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.metrics.IMetric;
import com.metrics.MetricComponent;

import java.util.Map;

/**
 * Created by tao on 27/07/15.
 */
public class ConstBolt extends BaseRichBolt{

    private OutputCollector _collector;
    private int _localIndex;
    private int _componentId;
    public long totalTransferred;
    public boolean original = false;

    public ConstBolt(String name, int taskNum){
        _componentId = MetricComponent.register(name, taskNum);
        original = true;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        _localIndex = topologyContext.getThisTaskIndex();
        //System.out.println("init -- "+_localIndex);
        totalTransferred = 0;
    }

    public void execute(Tuple tuple) {
        byte[] data = tuple.getBinary(0);
        MetricComponent.tick(_componentId, _localIndex, data.length);
        _collector.emit(new Values(data));
        //System.out.println("out --" + _componentId + "\t" + _localIndex + "\t" + original);
        totalTransferred += data.length;

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("object"));
    }

}
