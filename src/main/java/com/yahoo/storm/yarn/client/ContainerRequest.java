package com.yahoo.storm.yarn.client;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

public class ContainerRequest {
    Resource capability;
    String[] hosts;
    String[] racks;
    Priority priority;
    int containerCount;

    public ContainerRequest(Resource capability, String[] hosts,
            String[] racks, Priority priority, int containerCount) {
        this.capability = capability;
        this.hosts = (hosts != null ? hosts.clone() : null);
        this.racks = (racks != null ? racks.clone() : null);
        this.priority = priority;
        this.containerCount = containerCount;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Capability[").append(capability).append("]");
        sb.append("Priority[").append(priority).append("]");
        sb.append("ContainerCount[").append(containerCount).append("]");
        return sb.toString();
    }
}
