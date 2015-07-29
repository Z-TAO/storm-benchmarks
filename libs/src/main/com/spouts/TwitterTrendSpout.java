package com.spouts;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.metrics.MetricComponent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.security.spec.ECField;
import java.util.Map;
import java.util.Random;



public class TwitterTrendSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    private static int _Componentid;
    private int _index;
    private String [] data;


    public TwitterTrendSpout(String name, int NumTask) {
        _Componentid = MetricComponent.register(name, NumTask);
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _index = context.getThisTaskIndex();
        File f = new File("./random_text_generator.txt");
        byte [] stream = new byte[(int)f.length()];
        try{
            FileInputStream fi = new FileInputStream(f);
            fi.read(stream);
            String x = new String(stream);
            data = x.split(" ");

        }catch (Exception e){

        }

    }

    public void close() {

    }

    public void nextTuple() {
        //

        final Random rand = new Random();
        final String word = data[rand.nextInt(data.length)];
        MetricComponent.tick(_Componentid, _index, word.length());
        _collector.emit(new Values(word));
        //Utils.sleep(1);
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}