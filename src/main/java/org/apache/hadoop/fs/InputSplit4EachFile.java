package org.apache.hadoop.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class InputSplit4EachFile extends InputSplit implements Writable{
	
	private String fileName;
	private String[] locations;
	private int localSize =0;
	private long length =0;

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return locations;
	}

	public int getLocalSize() {
		return localSize;
	}

	public void setLocalSize(int localSize) {
		this.localSize = localSize;
	}

	public void setLocations(String[] locations) {
		this.locations = locations;
	}

	public void setLength(long length) {
		this.length = length;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(fileName);
		out.writeLong(length);
		if(locations!=null && locations.length>0){
			localSize=locations.length;
			out.writeInt(localSize);
			for(int i = 0 ;i<localSize;i++){
				out.writeUTF(locations[i]);
			}
		}else{
			out.writeInt(0);
		}
		
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		fileName=in.readUTF();
		length=in.readLong();
		localSize=in.readInt();
		if(localSize>0){
			locations=new String[localSize];
			for(int i=0;i<localSize;i++){
				locations[i]=in.readUTF();
			}
		}else{
			locations=new String[0];
		}
	}

}
