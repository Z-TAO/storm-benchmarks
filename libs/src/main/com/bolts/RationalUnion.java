package com.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.metrics.MetricComponent;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Z TAO on 7/29/2015.
 */
public class RationalUnion extends BaseRichBolt{
    private Map<String, String> coursesMarks = new HashMap<String, String>();
    private Map<String, String> names = new HashMap<String, String>();
    private int _compnondId;
    private int _index;

    public RationalUnion(String name, int taskNum){
       // _compnondId = MetricComponent.register(name, taskNum);
    }
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
       // _index = topologyContext.getThisTaskIndex();
    }

    @Override
    public void execute(Tuple tuple) {
        String id = tuple.getString(0);
        String data = tuple.getString(1);
       // System.out.println(id + "|||||" +data);
        {
            if (data.contains(" ")){
                //course
                String [] information = data.split(" +");
                if (coursesMarks.containsKey(id)){
                    coursesMarks.put(id, coursesMarks.get(id) +  "(" +information[0]+ ","+information[1]+ ")");
                }else{
                    coursesMarks.put(id, "(" +information[0]+ ","+information[1]+ ") " );
                }
            }else{
                //student
                if (!names.containsKey(id)){
                    names.put(id, data);
                }
            }
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("data"));
    }
}
