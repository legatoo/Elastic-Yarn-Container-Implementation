package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerMemoryStatus;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerMemoryStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerMemoryStatusProtoOrBuilder;

/**
 * Created by steven on 1/14/15.
 */

@Private
@Unstable
public class ContainerMemoryStatusPBImpl extends ContainerMemoryStatus {
    ContainerMemoryStatusProto proto = ContainerMemoryStatusProto.getDefaultInstance();
    ContainerMemoryStatusProto.Builder builder = null;
    boolean viaProto = false;

    private ContainerId containerId = null;


    public ContainerMemoryStatusPBImpl() {
        builder = ContainerMemoryStatusProto.newBuilder();
    }

    public ContainerMemoryStatusPBImpl(ContainerMemoryStatusProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public synchronized ContainerMemoryStatusProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
    }

    @Override
    public int hashCode() {
        return getProto().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other.getClass().isAssignableFrom(this.getClass())) {
            return this.getProto().equals(this.getClass().cast(other).getProto());
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ContainerMemoryStatus: [");
        sb.append("ContainerId: ").append(getContainerId()).append(", ");
        sb.append("VirtualMemory: ").append(getVirtualMemUsage()).append(", ");
        sb.append("PhysicalMemory: ").append(getPhysicalMemUsage()).append(" ");
        sb.append("]");
        return sb.toString();
    }

    private void mergeLocalToBuilder() {
        if (containerId != null) {
            builder.setContainerId(convertToProtoFormat(this.containerId));
        }
    }

    private synchronized void mergeLocalToProto() {
        if (viaProto)
            maybeInitBuilder();
        mergeLocalToBuilder();
        proto = builder.build();
        viaProto = true;
    }

    private synchronized void maybeInitBuilder() {
        if (viaProto || builder == null) {
            builder = ContainerMemoryStatusProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public synchronized void setContainerId(ContainerId containerId) {
        maybeInitBuilder();
        if (containerId == null)
            builder.clearContainerId();
        this.containerId = containerId;
    }

    @Override
    public synchronized ContainerId getContainerId() {
        ContainerMemoryStatusProtoOrBuilder p = viaProto ? proto : builder;
        if (this.containerId != null) {
            return this.containerId;
        }
        if (!p.hasContainerId()) {
            return null;
        }
        this.containerId =  convertFromProtoFormat(p.getContainerId());
        return this.containerId;
    }

    @Override
    public synchronized void setVirtualMemUsage(double virtualMemUsage) {
        maybeInitBuilder();
        builder.setVirtualMemUsage(virtualMemUsage);
    }

    @Override
    public synchronized double getVirtualMemUsage() {
        ContainerMemoryStatusProtoOrBuilder p = viaProto ? proto : builder;
        return (p.getVirtualMemUsage());
    }

    @Override
    public synchronized void setPhysicalMemUsage(double physicalMemUsage) {
        maybeInitBuilder();
        builder.setPhysicalMemUsage(physicalMemUsage);
    }

    @Override
    public synchronized double getPhysicalMemUsage() {
        ContainerMemoryStatusProtoOrBuilder p = viaProto ? proto : builder;
        return (p.getPhysicalMemUsage());
    }

    private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
        return new ContainerIdPBImpl(p);
    }

    private ContainerIdProto convertToProtoFormat(ContainerId t) {
        return ((ContainerIdPBImpl)t).getProto();
    }
}
