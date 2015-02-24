/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;

public class RMNodeStatusEvent extends RMNodeEvent {

    private final NodeHealthStatus nodeHealthStatus;
    private final List<ContainerStatus> containersCollection;
    private final NodeHeartbeatResponse latestResponse;
    private final List<ApplicationId> keepAliveAppIds;
    private final List<ContainerSqueezeUnit> squeezedContainers;
    private final List<ContainerId> containersToStretch;
    private final Resource stretchResourceSize;

    public RMNodeStatusEvent(NodeId nodeId, NodeHealthStatus nodeHealthStatus,
                             List<ContainerStatus> collection, List<ApplicationId> keepAliveAppIds,
                             NodeHeartbeatResponse latestResponse,
                             List<ContainerSqueezeUnit> squeezedContainers,
                             List<ContainerId> containersToStretch,
                             Resource stretchResourceSize) {
        super(nodeId, RMNodeEventType.STATUS_UPDATE);
        this.nodeHealthStatus = nodeHealthStatus;
        this.containersCollection = collection;
        this.keepAliveAppIds = keepAliveAppIds;
        this.latestResponse = latestResponse;
        this.squeezedContainers = squeezedContainers;
        this.stretchResourceSize = stretchResourceSize;
        this.containersToStretch = containersToStretch;
    }

    public NodeHealthStatus getNodeHealthStatus() {
        return this.nodeHealthStatus;
    }

    public List<ContainerStatus> getContainers() {
        return this.containersCollection;
    }

    public NodeHeartbeatResponse getLatestResponse() {
        return this.latestResponse;
    }

    public List<ApplicationId> getKeepAliveAppIds() {
        return this.keepAliveAppIds;
    }

    public List<ContainerSqueezeUnit> getSqueezedContainers() {
        return this.squeezedContainers;
    }

    public Resource getStretchResourceSize() {
        return stretchResourceSize;
    }

    public List<ContainerId> getContainersToStretch() {
        return containersToStretch;
    }
}