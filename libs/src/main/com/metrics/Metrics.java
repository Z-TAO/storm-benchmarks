package com.metrics;


import java.util.ArrayList;

/**
 * Created by tao on 28/07/15.
 */
public class Metrics {

    private ArrayList<IMetric> _registedObj = new ArrayList<IMetric>();
    public boolean _startTag = false;
    private long _lastSampleTime;
    private long SAMPLE_INTERVAL;
    private static final long DEFAULT_SAMPLE_INTERVAL = 1000;
    private ArrayList<Long []> _lastTimeData = new ArrayList<Long[]>();
    private ArrayList<Long []> _lastTimeDataMB = new ArrayList<Long[]>();
    private void printHeader(){
        System.out.println("Name\tTask Num\ttotal counts\tthroughput(MB/s)\tthroughput(tuples/s)\treceived size(MB)");
    }
    private void printData(String name, int taskNum, long totalCounts, double throughput, double throughputt, long receivedsize){
        System.out.print(String.format("%s\t%d\t%d\t%.3f\t%.3f\t%d\r\n", name, taskNum, totalCounts, throughput, throughputt, receivedsize));
    }
    private Thread _Monitor = new Thread(new Runnable() {
        public void run() {
            try{
                while (true){
                    if (System.currentTimeMillis() - _lastSampleTime > SAMPLE_INTERVAL && _startTag){
                        //sample once
                        int metricIndex = 0;
                        for (IMetric metric : _registedObj) {
                            String name = metric.getName();
                            long totalCounts = 0;
                            double speed = 0;
                            double speedMB = 0;
                            long [] totalCount = metric.getTotalCount();
                            long [] totalByte = metric.getTotalBytes();

                            for (int i = 0; i <totalCount.length; i++) {

                                speedMB += (double)(totalByte[i] - _lastTimeDataMB.get(metricIndex)[i])/SAMPLE_INTERVAL;
                                speed += (double)(totalCount[i] - _lastTimeData.get(metricIndex)[i])/SAMPLE_INTERVAL * 1000.0;
                                totalCounts += totalCount[i];
                                //totalBytes += totalByte[i];
                                _lastTimeData.get(metricIndex)[i] = totalCount[i];
                                _lastTimeDataMB.get(metricIndex)[i] = totalByte[i];
                            }

                            printData(name, totalCount.length, totalCounts, speedMB, speed, 0);
                            metricIndex++;
                        }
                        _lastSampleTime = System.currentTimeMillis();
                        Thread.sleep(SAMPLE_INTERVAL);
                    }else{
                        Thread.sleep(100);
                    }
                }
            }catch(InterruptedException e){
                System.out.println("Metrics Interrupted.");
            }
        }
    });

    public Metrics(long SampleInterval){
        _lastSampleTime = System.currentTimeMillis();
        SAMPLE_INTERVAL = SampleInterval;
        _Monitor.setPriority(Thread.MAX_PRIORITY);
        _Monitor.start();
    }

    public void register(Object o){
        _registedObj.add((IMetric)o);
        int size = ((IMetric) o).getTotalCount().length;
        Long [] data = new Long[size];
        Long [] dataMB = new Long[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = 0L;
            dataMB[i] = 0L;
        }
        _lastTimeDataMB.add(dataMB);
        _lastTimeData.add(data);
    }

    public void unregister(String id){
        //TODO: unregister the objects;
    }

    public void start(){
        _startTag = true;
        printHeader();
    }

    public void stop(){
        _startTag = false;
    }
}
