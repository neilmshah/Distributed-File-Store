package com.grpc.proxy;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.FileInfo;
import grpc.FileTransfer.FileMetaData;
import grpc.FileTransfer.FileUploadData;
import grpc.Team.ChunkLocations;
import io.grpc.stub.StreamObserver;

/**
 * Proxy Server Implmentation
 * @author Sricheta's computer
 *
 */
public class ProxyDataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase {

	ProxyClient proxyClient = new ProxyClient();

	final static Logger logger = Logger.getLogger(ProxyDataTransferServiceImpl.class);
	
	/**
	 * Upload chunks from Client to Proxy
	 * This method calls the db node to upload File chunk
	 * 
	 * rpc UploadFile (stream FileUploadData) returns (FileInfo); 
	 */
	@Override
	public StreamObserver<FileTransfer.FileUploadData> uploadFile(StreamObserver<FileTransfer.FileInfo> responseObserver) {

		return new StreamObserver<FileTransfer.FileUploadData>() {

			String fileName;

			@Override
			public void onNext(FileUploadData value) {
				fileName = value.getFileName();
				proxyClient.uploadDataToDB(value);
			}

			@Override
			public void onError(Throwable t) {
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

	/**
	 *  Download chunks from Client to Proxy
	 *  
	 * 	rpc DownloadChunk (ChunkInfo) returns (stream FileMetaData);
	 * 
	 */
	@Override
	public void downloadChunk(grpc.FileTransfer.ChunkInfo request,
	        io.grpc.stub.StreamObserver<grpc.FileTransfer.FileMetaData> responseObserver) {
		Timestamp ts1  =  new Timestamp(System.currentTimeMillis());
		
		ChunkLocations ch = proxyClient.GetChunkLocations(request);
			
		Iterator <FileMetaData> fileMetaDataList = proxyClient.downloadChunk(request, ch.getDbAddressesList());
		
		while(fileMetaDataList.hasNext()) {
			responseObserver.onNext(fileMetaDataList.next());
		}
		 responseObserver.onCompleted();
			Timestamp ts2  =  new Timestamp(System.currentTimeMillis());
			logger.debug("Method requestFileInfo ended at "+ ts2);
			logger.debug("Method downloadChunk ended at "+ ts2);
			logger.debug("Method downloadChunk execution time : "+ (ts2.getTime() - ts1.getTime()) + "ms");
	    }
}
