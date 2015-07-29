package com.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.metrics.MetricComponent;
import com.sun.deploy.util.ParameterUtil;
import com.utils.LineReader;

import javax.rmi.CORBA.Util;
import java.io.*;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by Z TAO on 7/29/2015.
 */

public class UnionSpout extends BaseRichSpout{

    private int _compnondId;
    private int _index;
    private SpoutOutputCollector _collector;
    private LineReader lr;
    private int lines;

    public UnionSpout(String name, int taskNum) throws IOException{
        _compnondId = MetricComponent.register(name, taskNum);
        lines = 0;
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _index = topologyContext.getThisTaskIndex();
        _collector = spoutOutputCollector;
        lines = 0;
        try{
            lr = new LineReader("./data/uniLarge"+_index+".txt");
        }catch (IOException e){
            System.exit(-2);
        }

    }

    @Override
    public void nextTuple() {
        try {
            String line = lr.getNextLine();

            if (line != null){
                lines ++;
                String [] seg = line.split(" +");
                if (seg[0].equals("student")){
                    _collector.emit(new Values(seg[1], seg[2]));
                }else if (seg[0].equals("mark")){
                    _collector.emit(new Values(seg[2], seg[1] + " " +seg[3]));
                }
                MetricComponent.tick(_compnondId, _index, seg.length);
            }else{
                //end
                System.out.println(lines);
                MetricComponent.stop();
                Utils.sleep(1000000);
            }
        }catch (IOException e){
            System.exit(-2);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id","data"));
    }
}
