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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.resource.Resources;


/**
 * Represents a YARN Cluster Node from the viewpoint of the scheduler.
 */
@Private
@Unstable
public abstract class SchedulerNode {

    private static final Log LOG = LogFactory.getLog(SchedulerNode.class);

    private Resource availableResource = Resource.newInstance(0, 0);
    private Resource usedResource = Resource.newInstance(0, 0);
    private Resource availableSqueezedResource = Resource.newInstance(0, 0);
    private Resource usedSqueezedResource = Resource.newInstance(0, 0);
    private Resource totalResourceCapability;
    private RMContainer reservedContainer;
    private volatile int numContainers;
    private volatile int numSqueezedContainers;
    private volatile int numSpeculativeContainers;


    /* set of containers that are allocated containers */
    private final Map<ContainerId, RMContainer> launchedContainers =
            new HashMap<ContainerId, RMContainer>();

    // TODO: squeezed containers
    private final Map<ContainerId, RMContainer> squeezedContainers =
            new HashMap<ContainerId, RMContainer>();

    private final Map<ContainerId, RMContainer> speculativeContainers =
            new HashMap<ContainerId, RMContainer>();

    private final RMNode rmNode;
    private final String nodeName;

    public SchedulerNode(RMNode node, boolean usePortForNodeName) {
        this.rmNode = node;
        this.availableResource = Resources.clone(node.getTotalCapability());
        this.totalResourceCapability = Resources.clone(node.getTotalCapability());
        if (usePortForNodeName) {
            nodeName = rmNode.getHostName() + ":" + node.getNodeID().getPort();
        } else {
            nodeName = rmNode.getHostName();
        }
    }

    public RMNode getRMNode() {
        return this.rmNode;
    }

    /**
     * Set total resources on the node.
     *
     * @param resource total resources on the node.
     */
    public synchronized void setTotalResource(Resource resource) {
        this.totalResourceCapability = resource;
        this.availableResource = Resources.subtract(totalResourceCapability,
                this.usedResource);
    }

    /**
     * Get the ID of the node which contains both its hostname and port.
     *
     * @return the ID of the node
     */
    public NodeId getNodeID() {
        return this.rmNode.getNodeID();
    }

    public String getHttpAddress() {
        return this.rmNode.getHttpAddress();
    }

    /**
     * Get the name of the node for scheduling matching decisions.
     * <p/>
     * Typically this is the 'hostname' reported by the node, but it could be
     * configured to be 'hostname:port' reported by the node via the
     * {@link YarnConfiguration#RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME} constant.
     * The main usecase of this is Yarn minicluster to be able to differentiate
     * node manager instances by their port number.
     *
     * @return name of the node for scheduling matching decisions.
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Get rackname.
     *
     * @return rackname
     */
    public String getRackName() {
        return this.rmNode.getRackName();
    }

    /**
     * The Scheduler has allocated containers on this node to the given
     * application.
     *
     * @param rmContainer allocated container
     */
    public synchronized void allocateContainer(RMContainer rmContainer) {

        boolean speculative = rmContainer.getContainer().getIfSpeculative();
        Container container = rmContainer.getContainer();

        if (speculative){
            Resource padding = container.getPadding();
            Resource capacity = container.getResource();

            deductAvailableSqueezedResource(padding);
            deductAvailableResource(Resources.subtract(capacity, padding));

            speculativeContainers.put(container.getId(), rmContainer);
            ++numSpeculativeContainers;

            LOG.info("Assigned one speculative container " + container.getId() +
                    " of capacity " + container.getResource() + " on host " + rmNode.getNodeAddress()
                    + ", which has " + container.getPadding() + " padding resource and cost"
                    + Resources.subtract(capacity, padding) +
                    " available resources. current available resource is "
                    + getAvailableResource() + " current used squeezed resource is " + getUsedSqueezedResource()
                    + ", available squeezed resource now is " + getAvailableSqueezedResource());
        } else {
            deductAvailableResource(container.getResource());
        }

        ++numContainers;
        launchedContainers.put(container.getId(), rmContainer);

        LOG.info("Assigned container " + container.getId() + " of capacity "
                + container.getResource() + " on host " + rmNode.getNodeAddress()
                + ", which has " + numContainers + " containers, "
                + getUsedResource() + " used and " + getAvailableResource()
                + " available after allocation");
    }

//    /**
//     * The Scheduler has allocated containers on this node to the given
//     * application.
//     *
//     * @param rmContainer allocated container
//     */
//    public synchronized void allocateContainerWithSqueezedSpace(RMContainer rmContainer) {
//
//        Container container = rmContainer.getContainer();
//        assert( container.getResource().getMemory() > getAvailableResource().getMemory());
//
//        // the priority is to use squeezed space first
//        if ( getSqueezedResource().getMemory() >= container.getResource().getMemory()){
//            deductSqueezedResource(container.getResource());
//            container.setPadding(container.getResource());
//        } else {
//            Resource diff = Resources.subtract(container.getResource(), getSqueezedResource());
//
//            deductAvailableResource(diff);
//            container.setPadding(getSqueezedResource());
//
//            // squeezed resource is used up
//            squeezedResource.setMemory(0);
//            squeezedResource.setVirtualCores(0);
//
//        }
//
//        ++numSqueezedContainers;
//        ++numContainers;
//
//        launchedContainers.put(container.getId(), rmContainer);
//        speculativeContainers.put(container.getId(), rmContainer);
//
//        LOG.info("Assigned speculative container " + container.getId() + " of capacity "
//                + container.getResource() + " on host " + rmNode.getNodeAddress()
//                + ", which has " + numContainers + " containers, "
//                + getUsedResource() + " used and " + getSqueezedResource() + " squeezed used and "
//                + getAvailableResource()
//                + " available and " + getSqueezedResource() + " available squeezed space after allocation");
//    }


    /**
     * Get available resources on the node.
     *
     * @return available resources on the node
     */
    public synchronized Resource getAvailableResource() {
        //TODO: HAKUNAMI
        return this.availableResource;
        //return Resources.add(this.availableResource, this.squeezedResource);
    }

    /**
     * Get used resources on the node.
     *
     * @return used resources on the node
     */
    public synchronized Resource getUsedResource() {
        return this.usedResource;
    }

    public synchronized Resource getUsedSqueezedResource(){
        return this.usedSqueezedResource;
    }

    public synchronized Resource getAvailableSqueezedResource(){
        return this.availableSqueezedResource;
    }

    /**
     * Get total resources on the node.
     *
     * @return total resources on the node.
     */
    public synchronized Resource getTotalResource() {
        return this.totalResourceCapability;
    }

    public synchronized boolean isValidContainer(ContainerId containerId) {
        if (launchedContainers.containsKey(containerId)) {
            return true;
        }
        return false;
    }

    private synchronized void updateResource(Container container) {

        if (container.getIfSpeculative()){
            addAvailableResource(Resources.subtract(
                    container.getResource(), container.getPadding()));
            releaseSqueezedResource(container.getPadding());
            --numSpeculativeContainers;
        } else {
            addAvailableResource(container.getResource());
        }

        --numContainers;
    }

    /**
     * Release an allocated container on this node.
     *
     * @param container container to be released
     */
    public synchronized void releaseContainer(Container container) {
        if (!isValidContainer(container.getId())) {
            LOG.error("Invalid container released " + container);
            return;
        }

    /* remove the containers from the nodemanger */
        if (null != launchedContainers.remove(container.getId())) {

            if (squeezedContainers.containsKey(container.getId()))
                squeezedContainers.remove(container.getId());

            if (speculativeContainers.containsKey(container.getId())){
                speculativeContainers.remove(container.getId());
            }

            updateResource(container);
        }


        LOG.info("Released container " + container.getId() + " of capacity "
                + container.getResource() + " on host " + rmNode.getNodeAddress()
                + ", which currently has " + numContainers + " containers, "
                + getUsedResource() + " used and " + getAvailableResource()
                + " available" + ", release resources=" + true);
    }

    /**
     * Squeeze an allocated container on this node
     *
     * @param rmContainer
     * @param containerSqueezeUnit
     */
    public synchronized void squeezeContainer(RMContainer rmContainer,
                                              ContainerSqueezeUnit containerSqueezeUnit) {
        // put rmContainer into squeezedContainers
        if (!squeezedContainers.containsKey(containerSqueezeUnit.getContainerId())) {
            squeezedContainers.put(containerSqueezeUnit.getContainerId(), rmContainer);
            numSqueezedContainers++;
            addAvailableSqueezedResource(containerSqueezeUnit.getDiff());

            LOG.debug("Add squeeze diff " + containerSqueezeUnit.getDiff() + " from " +
                    containerSqueezeUnit.getContainerId() + " to node resource. ");
        }

        LOG.debug("Current available squeezed resource is " + getAvailableSqueezedResource());
    }

    private synchronized void addAvailableResource(Resource resource) {
        if (resource == null) {
            LOG.error("Invalid resource addition of null resource for "
                    + rmNode.getNodeAddress());
            return;
        }
        Resources.addTo(availableResource, resource);
        Resources.subtractFrom(usedResource, resource);
    }

    private synchronized void addAvailableSqueezedResource(Resource resource) {
        if (resource == null) {
            LOG.error("Invalid resource addition of null resource for "
                    + rmNode.getNodeAddress());
            return;
        }
        Resources.addTo(availableSqueezedResource, resource);
    }

    private synchronized void releaseSqueezedResource(Resource resource) {
        if (resource == null) {
            LOG.error("Invalid resource addition of null resource for "
                    + rmNode.getNodeAddress());
            return;
        }
        Resources.addTo(availableSqueezedResource, resource);
        Resources.subtract(usedSqueezedResource, resource);
    }


    private synchronized void deductAvailableResource(Resource resource) {
        if (resource == null) {
            LOG.error("Invalid deduction of null resource for "
                    + rmNode.getNodeAddress());
            return;
        }
        Resources.subtractFrom(availableResource, resource);
        Resources.addTo(usedResource, resource);
    }

    private synchronized void deductAvailableSqueezedResource(Resource resource) {
        if (resource == null) {
            LOG.error("Invalid deduction of null resource for "
                    + rmNode.getNodeAddress());
            return;
        }
        Resources.subtractFrom(availableSqueezedResource, resource);
        Resources.addTo(usedSqueezedResource, resource);
    }

    /**
     * Reserve container for the attempt on this node.
     */
    public abstract void reserveResource(SchedulerApplicationAttempt attempt,
                                         Priority priority, RMContainer container);

    /**
     * Unreserve resources on this node.
     */
    public abstract void unreserveResource(SchedulerApplicationAttempt attempt);

    @Override
    public String toString() {
        return "host: " + rmNode.getNodeAddress() + " #containers="
                + getNumContainers() + " available="
                + getAvailableResource().getMemory() + " used="
                + getUsedResource().getMemory();
    }

    /**
     * Get number of active containers on the node.
     *
     * @return number of active containers on the node
     */
    public int getNumContainers() {
        return numContainers;
    }

    public synchronized List<RMContainer> getRunningContainers() {
        return new ArrayList<RMContainer>(launchedContainers.values());
    }

    public synchronized RMContainer getReservedContainer() {
        return reservedContainer;
    }

    protected synchronized void
    setReservedContainer(RMContainer reservedContainer) {
        this.reservedContainer = reservedContainer;
    }

    public synchronized void recoverContainer(RMContainer rmContainer) {
        if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
            return;
        }
        allocateContainer(rmContainer);
    }

    public synchronized void updateResourceAfterSqueeze(List<ContainerSqueezeUnit> containers){
        // TODO: update available resource here
        LOG.debug("Update resource in SchedulerNode: ");
        for (ContainerSqueezeUnit csu : containers){
            LOG.debug("Before: " + getAvailableResource());
            Resource diff = csu.getDiff();
            Resources.addTo(availableResource, diff);
            Resources.addTo(availableSqueezedResource, diff);
            LOG.debug("After: " + getAvailableResource());
        }
    }

    // TODO: update resource information after container covery operation
}
