package org.apache.hadoop.yarn.server.resourcemanager.periodicservice;

/**
 * Created by steven on 1/20/15.
 */
public enum PeriodicSchedulerEventType {
    MEMORY_STATUSES_UPDATE,

    OPERATE_PERIODIC_SCHEDULE,

    SQUEEZE_DONE,

    STRETCH_DONE
}
