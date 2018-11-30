package com.grpc.proxy;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.ChunkInfo;
import grpc.FileTransfer.FileInfo;
import grpc.FileTransfer.FileMetaData;
import grpc.FileTransfer.FileUploadData;
import io.grpc.stub.StreamObserver;

/**
 * Test Proxy DB class
 * @author Sricheta's computer
 *
 */
public class TestDBDataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase{

	final static Logger logger = Logger.getLogger(TestDBDataTransferServiceImpl.class);
	@Override
	public StreamObserver<FileTransfer.FileUploadData> uploadFile(StreamObserver<FileTransfer.FileInfo> responseObserver) {

		return new StreamObserver<FileTransfer.FileUploadData>() {

			String fileName;

			@Override
			public void onNext(FileUploadData value) {
				fileName = value.getFileName();
				System.out.println(value+"fffd");
			}

			@Override
			public void onError(Throwable t) {
				t.printStackTrace();
				
				logger.error("Error occured in uploadFile" + t.getMessage());
				responseObserver.onError(t);
			}

			@Override
			public void onCompleted() {
				responseObserver.onNext(FileInfo.newBuilder()
						.setFileName(fileName)
						.build());
				responseObserver.onCompleted();
			}

		};
	}
	
	
	@Override
	public void downloadChunk(grpc.FileTransfer.ChunkInfo request,
	        io.grpc.stub.StreamObserver<grpc.FileTransfer.FileMetaData> responseObserver) {
		FileMetaData fmd =  FileMetaData.newBuilder().setChunkId(1).setFileName("Test")
				.setSeqNum(request.getStartSeqNum()).setSeqMax(3).setData(ByteString.copyFromUtf8("Hello World!!")).build();
		FileMetaData fmd1 =  FileMetaData.newBuilder().setChunkId(1).setFileName("Test")
				.setSeqNum(1).setSeqMax(3).setData(ByteString.copyFromUtf8("Outdoor Testing")).build();
		FileMetaData fmd2 =  FileMetaData.newBuilder().setChunkId(1).setFileName("Test")
				.setSeqNum(2).setSeqMax(3).setData(ByteString.copyFromUtf8("MacBook Air")).build();
		responseObserver.onNext(fmd);
		responseObserver.onNext(fmd1);
		responseObserver.onNext(fmd2);
		responseObserver.onCompleted();
	   }
}
