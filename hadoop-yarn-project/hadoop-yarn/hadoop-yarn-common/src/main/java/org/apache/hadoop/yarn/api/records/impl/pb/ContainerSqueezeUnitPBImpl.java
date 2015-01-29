package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerSqueezeUnitProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerSqueezeUnitProtoOrBuilder;


/**
 * Created by steven on 1/20/15.
 */
public class ContainerSqueezeUnitPBImpl extends ContainerSqueezeUnit {

    ContainerSqueezeUnitProto proto = ContainerSqueezeUnitProto.getDefaultInstance();
    ContainerSqueezeUnitProto.Builder builder = null;
    boolean viaProto = false;

    private ContainerId containerId = null;

    public ContainerSqueezeUnitPBImpl() {
        builder = ContainerSqueezeUnitProto.newBuilder();
    }

    public ContainerSqueezeUnitPBImpl(ContainerSqueezeUnitProto proto){
        this.proto = proto;
        viaProto = true;
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ContainerSqueezeUnit: [");
        sb.append("ContainerId: ").append(getContainerId()).append(", ");
        sb.append("Origin: ").append(getOrigin()).append(", ");
        sb.append("Target: ").append(getTarget()).append(" ");
        sb.append("]");
        return sb.toString();
    }

    public synchronized ContainerSqueezeUnitProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
    }

    private void maybeInitBuilder() {
        if (viaProto || builder == null) {
            builder = ContainerSqueezeUnitProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public void setContainerId(ContainerId containerId) {
        maybeInitBuilder();
        if ( containerId == null)
            builder.clearContainerId();
        this.containerId = containerId;

    }

    @Override
    public void setOrigin(Resource origin) {
        maybeInitBuilder();
        builder.setOrigin(convertToProtoFormat(origin));

    }

    @Override
    public void setTarget(Resource target) {
        maybeInitBuilder();
        builder.setTarget(convertToProtoFormat(target));
    }

    @Override
    public ContainerId getContainerId() {
        ContainerSqueezeUnitProtoOrBuilder p = viaProto ? proto : builder;
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
    public Resource getOrigin() {
        ContainerSqueezeUnitProtoOrBuilder p = viaProto ? proto : builder;
        return convertFromProtoFormat(p.getOrigin());
    }

    @Override
    public Resource getTarget() {
        ContainerSqueezeUnitProtoOrBuilder p = viaProto ? proto : builder;
        return convertFromProtoFormat(p.getTarget());
    }

    private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
        return new ContainerIdPBImpl(p);
    }

    private ContainerIdProto convertToProtoFormat(ContainerId t) {
        return ((ContainerIdPBImpl)t).getProto();
    }

    private ResourceProto convertToProtoFormat (Resource r) {
        return ((ResourcePBImpl) r).getProto();
    }

    private ResourcePBImpl convertFromProtoFormat (ResourceProto p) {
        return new ResourcePBImpl(p);
    }
}
