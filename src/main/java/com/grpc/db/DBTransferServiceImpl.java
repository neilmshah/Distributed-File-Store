package com.grpc.db;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.FileInfo;
import grpc.FileTransfer.FileMetaData;
import grpc.FileTransfer.FileUploadData;
import io.grpc.stub.StreamObserver;

/**
 * This DB Server Implementation
 * @author Sricheta's computer
 *
 */
public class DBTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase {

	private ConcurrentHashMap<String, FileData> db;
	private final String KEY_DELIMINATOR= "#";
	
	public DBTransferServiceImpl(ConcurrentHashMap<String , byte[]> map){
		super();
		db= new ConcurrentHashMap<String , FileData>();
	}

	final static Logger logger = Logger.getLogger(DBTransferServiceImpl.class);
	@Override
	public StreamObserver<FileTransfer.FileUploadData> uploadFile(StreamObserver<FileTransfer.FileInfo> responseObserver) {

		return new StreamObserver<FileTransfer.FileUploadData>() {

			byte[] bytes;
			String key; 
			String filename;
			long chunkId;

			@Override
			public void onNext(FileUploadData value) {
				filename = value.getFileName();
				chunkId = value.getChunkId();
				bytes = value.getFileNameBytes().toByteArray();
				key = value.getFileName()+KEY_DELIMINATOR+value.getChunkId()+KEY_DELIMINATOR+value.getSeqNum();
				
				db.put(key, new FileData(value.getSeqMax(), value.getData().toByteArray()));
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
				logger.error("File "+ filename + chunkId  + " ploaded successfully to the DB hashmap ");
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
	public void downloadChunk(grpc.FileTransfer.ChunkInfo request, io.grpc.stub.StreamObserver<grpc.FileTransfer.FileMetaData> responseObserver) {
		
		
		for (Entry<String, FileData> entry : db.entrySet()) {
			logger.debug("DB Key => " +entry.getKey() +  "  DB Value  => " + entry.getValue());
	        if (entry.getKey().startsWith(request.getFileName()+KEY_DELIMINATOR+request.getChunkId())) {
	        	  
	        	 responseObserver.onNext(
	    				 FileMetaData.newBuilder()
	    					.setFileName(request.getFileName())
	    					.setChunkId(request.getChunkId())
	    					.setSeqNum(request.getStartSeqNum())
	    					.setData(ByteString.copyFrom(entry.getValue().getData()))
	    					.setSeqMax(entry.getValue().getSeqMax())
	    					.build()
	    		);
	        	
	        }
	    }
		
		
		 responseObserver.onCompleted();
		
	
	}


}
