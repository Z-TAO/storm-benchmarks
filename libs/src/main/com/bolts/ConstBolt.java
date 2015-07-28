package com.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.metrics.IMetric;

import java.util.Map;

/**
 * Created by tao on 27/07/15.
 */
public class ConstBolt extends BaseRichBolt implements IMetric{

    private static long [] _totalReceivedBytes;
    private static long [] _totalCounts;

    private OutputCollector _collector;
    private String _name;

    private int _currentIndex;
    public ConstBolt(String name, int taskNum){
        _totalReceivedBytes = new long[taskNum];
        _totalCounts = new long[taskNum];
        _name = name;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        //_name = topologyContext.getThisComponentId();
        _currentIndex = topologyContext.getThisTaskIndex();

    }

    public void execute(Tuple tuple) {
        byte[] data = tuple.getBinary(0);
        _totalReceivedBytes[_currentIndex] += data.length;
        _totalCounts[_currentIndex] ++;
        _collector.emit(new Values(data));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("object"));
    }

    @Override
    public long[] getTotalCount() {
        return _totalCounts;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public long[] getTotalBytes() {
        return _totalReceivedBytes;
    }
}
