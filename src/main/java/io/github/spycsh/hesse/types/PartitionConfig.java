package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PartitionConfig {
    @JsonProperty("partition_id")
    private String PartitionId;

    PartitionConfig(){};

    public String getPartitionId() {
        return PartitionId;
    }

    public void setPartitionId(String partitionId) {
        PartitionId = partitionId;
    }
}
