package com.grpc.db;

public class FileData {

	private byte[] data;
	private long  seqMax;

	public FileData(long seqMax, byte[] data) {
		this.seqMax = seqMax;
		this.data = data;
	}
	public byte[] getData() {
		return data;
	}
	public void setData(byte[] data) {
		this.data = data;
	}
	public long getSeqMax() {
		return seqMax;
	}
	public void setSeqMax(long seqMax) {
		this.seqMax = seqMax;
	}

}
