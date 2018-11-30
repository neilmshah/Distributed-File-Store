package com.grpc.db;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.grpc.proxy.TestDBDataTransferServiceImpl;
import com.grpc.raft.RaftServer;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.FileInfo;
import grpc.FileTransfer.FileUploadData;
import io.grpc.stub.StreamObserver;

/**
 * This DB Server Implementation
 * @author Sricheta's computer
 *
 */
public class DBTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase {

	private ConcurrentHashMap<String , byte[]> db;
	
	public DBTransferServiceImpl(ConcurrentHashMap<String , byte[]> map){
		super();
		db= new ConcurrentHashMap<String , byte[]>();
	}

	final static Logger logger = Logger.getLogger(DBTransferServiceImpl.class);
	@Override
	public StreamObserver<FileTransfer.FileUploadData> uploadFile(StreamObserver<FileTransfer.FileInfo> responseObserver) {

		return new StreamObserver<FileTransfer.FileUploadData>() {

			byte[] bytes;
			String key; 
			String filename;

			@Override
			public void onNext(FileUploadData value) {
				filename = value.getFileName();
				bytes = value.getFileNameBytes().toByteArray();
				key = value.getFileName()+"_"+value.getChunkId();
				db.put(key, bytes);
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
						.setFileName(filename)
						.build());
				logger.error("File "+ filename +"Uploaded successfully to the DB hashmap ");
				responseObserver.onCompleted();
			}

		};
	}


}
