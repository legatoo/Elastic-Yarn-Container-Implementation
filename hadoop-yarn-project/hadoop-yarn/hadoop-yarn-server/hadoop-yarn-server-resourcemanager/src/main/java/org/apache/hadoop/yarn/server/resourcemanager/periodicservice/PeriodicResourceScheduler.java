package org.apache.hadoop.yarn.server.resourcemanager.periodicservice;

/**
 * Created by steven on 1/20/15.
 */

import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;
import java.util.List;

/**
 * 1. Receive container memory statuses among cluster from heartbeats
 * 2. Periodically choose proper containers to be squeezed from the list
 * 3. Send CONTAINER_SQUEEZE event to RM's Dispacther
 */
public interface PeriodicResourceScheduler  {
    public List<ContainerSqueezeUnit> getContainersToSqueezed();

    public void dispatchSqueezeEvent();

//    public void updateNodeHeartbeatResponseForSqueeze(NodeHeartbeatResponse response);

}
