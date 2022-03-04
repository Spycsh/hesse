package io.github.spycsh.hesse.types;

public class VertexActivity {
    private String activityType;

    private String srcId;
    private String dstId;
    private String weight;
    private String timestamp;

    public VertexActivity() {}

    public VertexActivity(String activityType, String srcId, String dstId, String timestamp) {
        this.activityType = activityType;
        this.srcId = srcId;
        this.dstId = dstId;
        this.timestamp = timestamp;
    }

    public VertexActivity(String activityType, String srcId, String dstId, String weight, String timestamp) {
        this.activityType = activityType;
        this.srcId = srcId;
        this.dstId = dstId;
        this.weight = weight;
        this.timestamp = timestamp;
    }

    public void setWeight(String weight) {
        this.weight = weight;
    }

    public String getWeight() {
        return weight;
    }

    public String getActivityType() {
        return activityType;
    }

    public void setActivityType(String activityType) {
        this.activityType = activityType;
    }

    public String getSrcId() {
        return srcId;
    }

    public void setSrcId(String srcId) {
        this.srcId = srcId;
    }

    public String getDstId() {
        return dstId;
    }

    public void setDstId(String dstId) {
        this.dstId = dstId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
