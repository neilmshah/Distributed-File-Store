package com.grpc.raft;

import java.util.List;

public class ChunkLocation {

	private String fileName;
	private int chunkIndex;
	private List<String> databaseAddress;

	public ChunkLocation(String fname, int ci, List<String> dbaddr){
		fileName = fname;
		chunkIndex = ci;
		databaseAddress = dbaddr;
	}
}
