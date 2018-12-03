package com.grpc.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.protobuf.ByteString;

import grpc.DataTransferServiceGrpc.DataTransferServiceStub;
import grpc.FileTransfer;
import grpc.FileTransfer.FileInfo;
import grpc.FileTransfer.FileUploadData;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

public class FileRead implements Runnable {

	private FileChannel channel;
	private long startLocation;
	private int size;
	File f;
	int shard_num;
	Map<DataTransferServiceStub, List<Integer>> map;
	FileInputStream fis = null;
	int seqSize = 0;
	int seqMax  =0;
	int maxChunks;
	long totalSeq;
	
	
	public FileRead(long start_loc, int seqSize, long chunk_size, File f, FileChannel ch,  int i, Map<DataTransferServiceStub, List<Integer>> multiThreadMap, int seqMax, int maxChunks, long totalSeq) throws FileNotFoundException {
		startLocation = start_loc;
		this.size = size;
		shard_num = i;
		this.f = f;
		this.seqSize  = seqSize;
		map = multiThreadMap;
		channel = ch;
		this.seqMax = seqMax;
		this.maxChunks = maxChunks;
		this.totalSeq = totalSeq;
	}

	@Override
	public void run() {
		
		StreamObserver<FileInfo> responseObserver = new StreamObserver<FileInfo>() {
			@Override
			public void onNext(FileInfo fileInfo) {

			}

			@Override
			public void onError(Throwable t) {
				t.printStackTrace();

			}

			@Override
			public void onCompleted() {
				System.out.println("On completed");
			}
		};
		
		ByteBuffer buff = ByteBuffer.allocate(size);
		try {
			channel.read(buff, startLocation);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		InputStream bis = new ByteArrayInputStream(buff.array());
		
		byte[] buffer = new byte[seqSize];
		int seqNum =0;
		DataTransferServiceStub key = (DataTransferServiceStub)map.keySet().toArray()[shard_num];
		List<Integer> chunks = map.get(key);
		int startChunkLocation = chunks.get(0);
		int size = chunks.size();
	
		int bytesAmount = 0;
		
		try {
			while ((bytesAmount = bis.read(buffer)) > 0) {
				StreamObserver<FileTransfer.FileUploadData> requestObserver = key.uploadFile(responseObserver);
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				seqNum++;
				out.write(buffer, 0, bytesAmount);	
				byte[] contents = out.toByteArray();

				FileUploadData uploadData = FileUploadData.newBuilder().setFileName(f.getName())
						.setChunkId(startChunkLocation).setMaxChunks(maxChunks)
						.setSeqNum(seqNum).setSeqMax(seqMax).setData(ByteString.copyFrom(contents)).build();
				
				requestObserver.onNext(uploadData);
				try {
					Thread.sleep(new Random().nextInt(1000) + 500);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if((seqNum == seqMax && startChunkLocation < maxChunks) || maxChunks == 1) {
					requestObserver.onCompleted();
					((ManagedChannel) key.getChannel()).shutdown();
					seqNum = 0;
					startChunkLocation++;
				} else if(seqNum == (totalSeq % (maxChunks-1)) && startChunkLocation == maxChunks){
					requestObserver.onCompleted();
					((ManagedChannel)key.getChannel()).shutdown();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

}
