package com.metrics;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.lmax.disruptor.RingBuffer;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.security.acl.LastOwnerException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by Z TAO on 7/29/2015.
 */
public class RollingCountBolt extends BaseRichBolt {

    private static int SLICE_SIZE = 10000;
    private Map<String, Integer> _block_count = new HashMap<String, Integer>();
    private LinkedList<String> buffer = new LinkedList<String>();
    private LinkedList<Long> time = new LinkedList<Long>();

    private int _componentId;
    private int _index;
    private long _lastUpdateTime;

    public RollingCountBolt(String name, int numTask){
        _componentId = MetricComponent.register(name, numTask);
        _lastUpdateTime = System.currentTimeMillis();
    }

    private void addWord(String word){
        if (_block_count.containsKey(word)){
            _block_count.put(word, _block_count.get(word) + 1);
        }else{
            _block_count.put(word, 1);
        }
    }
    private void reduceWord(String word){
        if (_block_count.get(word) == 1){
            _block_count.remove(word);
        }else{
            _block_count.put(word, _block_count.get(word) - 1);
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _index = topologyContext.getThisTaskIndex();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        buffer.add(word);
        time.add(System.currentTimeMillis());
        addWord(word);
        while (!time.isEmpty() && System.currentTimeMillis() - time.getFirst() > SLICE_SIZE){
            reduceWord(buffer.getFirst());
            time.removeFirst();
            buffer.removeFirst();
        }
        MetricComponent.tick(_componentId, _index, word.length());

        if (System.currentTimeMillis() - _lastUpdateTime > 1000){
            //update
           // System.out.println(_block_count.toString());
            _lastUpdateTime = System.currentTimeMillis();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
