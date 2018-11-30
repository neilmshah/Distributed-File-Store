package com.grpc.db;

public class Metadata {

    public Boolean isAvailable;
    public long chunkId;
    public long maxChunks;
    public String filepath;
    public long filesize;

    Metadata(long chunkId, long maxChunks, boolean isAvailable, String filepath, long filesize){
        this.chunkId = chunkId;
        this.maxChunks = maxChunks;
        this.isAvailable = isAvailable;
        this.filepath = filepath;
        this.filesize = filesize;
    }
}
