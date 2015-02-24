package org.apache.hadoop.yarn.server.nodemanager.containermanager.squeezer;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by steven on 2/24/15.
 */
public class ContainerStretchEvent extends ContainerSqueezerBaseEvent {

    private final Resource stretchResourceSize;
    private final List<ContainerId> containersNeedToStretch;

    public ContainerStretchEvent(List<ContainerId> containers, Resource resource) {
        super(ContainerSqueezerEventType.STRETCH_RESOURCE);
        if (resource.getMemory() == 0)
            this.stretchResourceSize = Resource.newInstance(0, 0);
        else
            this.stretchResourceSize = resource;
        containersNeedToStretch = new ArrayList<ContainerId>();
        containersNeedToStretch.addAll(containers);
    }

    public Resource getStrecthResourceSize() {
        return stretchResourceSize;
    }

    public List<ContainerId> getContainersNeedToStretch() {
        return containersNeedToStretch;
    }
}

