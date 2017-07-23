package be.hoekx.hbase;

public class TagId {
    private final long tagId;

    public TagId(long tagId) {
        this.tagId = tagId;
    }

    long getTagId() {
        return tagId;
    }
}
