package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;

import java.util.List;

/**
 * Created by steven on 2/3/15.
 */
public class CMgrSuqeezeEvent extends ContainerManagerEvent {

    private final List<ContainerSqueezeUnit> containerToBeSqueezed;
    private final Reason reason;

    public CMgrSuqeezeEvent(List<ContainerSqueezeUnit> containerToBeSqueezed,
                                        Reason reason) {
        super(ContainerManagerEventType.SQUEEZE_CONTAINERS);
        this.containerToBeSqueezed = containerToBeSqueezed;
        this.reason = reason;
    }

    public List<ContainerSqueezeUnit> getContainersToBeSqueezed() {
        return this.containerToBeSqueezed;
    }

    public Reason getReason() {
        return reason;
    }

    public static enum Reason {
        /**
         * When container's memory usage is relative low for sometime
         */
        ON_PERIODICAL_SCHEDULER
    }

}
