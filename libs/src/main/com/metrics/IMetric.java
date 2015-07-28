package com.metrics;

/**
 * Created by tao on 28/07/15.
 */
public interface IMetric {

    long [] getTotalCount();
    String getName();
    long [] getTotalBytes();

}
