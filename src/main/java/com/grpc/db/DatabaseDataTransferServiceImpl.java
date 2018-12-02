package com.grpc.db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import io.grpc.stub.StreamObserver;

public class DatabaseDataTransferServiceImpl extends DataTransferServiceGrpc.DataTransferServiceImplBase {

	private final String KEY_DELIMINATOR= "#";
	 final static Logger logger = Logger.getLogger(DatabaseDataTransferServiceImpl.class);
	    public ConcurrentHashMap<String, Metadata> fileMap;

	    DatabaseDataTransferServiceImpl(){
	        fileMap = new ConcurrentHashMap<>();
	    }

	    public StreamObserver<FileTransfer.FileUploadData> uploadFile (StreamObserver<FileTransfer.FileInfo> responseObserver){
	        return new StreamObserver<FileTransfer.FileUploadData>() {
	            long lastSeqNum = 0;
	            long seqMax = 0;
	            long chunkId;
	            long maxChunks;

	            ByteString accumulator = ByteString.EMPTY;

	            String filename = new String();
	            @Override
	            public void onNext(FileTransfer.FileUploadData fileUploadData) {
	                // accumulate parts
	                // form a chunk
	                if(lastSeqNum == 0) {
	                    filename = fileUploadData.getFileName();
	                    chunkId = fileUploadData.getChunkId ();
	                    seqMax = fileUploadData.getSeqMax();
	                    maxChunks = fileUploadData.getMaxChunks();
	                }

	                accumulator.concat(fileUploadData.getData());
	                lastSeqNum = fileUploadData.getSeqNum();
	            }

	            @Override
	            public void onError(Throwable throwable) {
	                logger.error("Upload file failed. "+throwable.getMessage());
	            }

	            @Override
	            public void onCompleted() {
	                //create an entry in the table
	                Metadata newEntry = new Metadata(chunkId, maxChunks, true, "C:\\data\\SJSU2ndSem\\Gash275\\cmpe275\\test\\", accumulator.size());
	                String filename_chunkId = filename+KEY_DELIMINATOR+chunkId;
	                fileMap.put(filename+KEY_DELIMINATOR+chunkId,newEntry);
	                //get the data and write it to disk
	                try{
	                    File file = new File("C:\\data\\SJSU2ndSem\\Gash275\\cmpe275\\test\\"+filename_chunkId);
	                    OutputStream is = new FileOutputStream(file);
	                    //flush file to disk
	                    is.write(accumulator.toByteArray());
	                    is.close();
	                } catch (FileNotFoundException e) {
	                    e.printStackTrace();
	                } catch (IOException e) {
	                    e.printStackTrace();
	                }
	                responseObserver.onCompleted();
	            }
	        };
	    }

	    public void downloadChunk (FileTransfer.ChunkInfo req, StreamObserver<FileTransfer.FileMetaData> responseObserver){
	        // break chunk into smaller parts and stream them.
	        responseObserver.onNext(getChunk(req));
	        responseObserver.onCompleted();
	    }

	    /**
	     * Helper function to create response chunks
	     * @param req
	     * @return response for downloadChunk
	     */
	    private FileTransfer.FileMetaData getChunk(FileTransfer.ChunkInfo req) {
	        //unpack request
	        String fileName = req.getFileName();
	        Long chunkId = req.getChunkId();

	        //create key
	        String fullFileName = fileName+KEY_DELIMINATOR+chunkId.toString();

	        //find key in fileMap
	        String path = fileMap.get(fullFileName).filepath;
	        long maxChunks = fileMap.get(fullFileName).maxChunks;
	        long fileSize = fileMap.get(fullFileName).filesize;

	        //read file
	        long chunkSize = fileSize/maxChunks;
	        byte[] data = {};
	        try{
	            File file = new File(path);
	            int size = (int) file.length();
	            InputStream is = new FileInputStream(file);
	            is.read(data);
	            is.close();
	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }

	        //create chunk
	        byte[] chunkdata = {};
	        for(int i=0; i< chunkSize; i++){
	            chunkdata[i] = data[i];
	        }

	        //create response (whole chunk at once - no sequence stream)
	        ByteString chunk = ByteString.copyFrom(chunkdata);
	        FileTransfer.FileMetaData response = FileTransfer.FileMetaData.newBuilder()
	                .setChunkId(chunkId)
	                .setFileName(fileName)
	                .setSeqNum(0)
	                .setSeqMax(1)
	                .setData(chunk)
	                .build();

	        return response;
	    }
	    
}
