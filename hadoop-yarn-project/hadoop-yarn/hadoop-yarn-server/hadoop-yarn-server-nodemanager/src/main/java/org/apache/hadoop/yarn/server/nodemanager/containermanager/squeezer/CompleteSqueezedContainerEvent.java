package org.apache.hadoop.yarn.server.nodemanager.containermanager.squeezer;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;
import org.apache.hadoop.yarn.event.AbstractEvent;

import java.util.List;

/**
 * Created by steven on 2/24/15.
 */
public class CompleteSqueezedContainerEvent extends ContainerSqueezerBaseEvent {

    private final ContainerId completedSqueezedContainerId;

    public CompleteSqueezedContainerEvent(ContainerId container) {
        super(ContainerSqueezerEventType.REMOVE_SQUEEZED_CONTAINER);
        this.completedSqueezedContainerId = container;
    }

    public ContainerId getCompletedSqueezedContainerId() {
        return completedSqueezedContainerId;
    }

}
