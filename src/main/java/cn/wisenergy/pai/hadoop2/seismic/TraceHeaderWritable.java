package cn.wisenergy.pai.hadoop2.seismic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

public class TraceHeaderWritable implements WritableComparable<TraceHeaderWritable>{
	
	private byte[] headerBytes = {};
	private int length=0;
	private int[] keys;
	private int keyLength=0;
	
	

	public byte[] getHeaderBytes() {
		return headerBytes;
	}

	public void setHeaderBytes(byte[] b) {
		length=b.length;
		this.headerBytes = new byte[length];
		System.arraycopy(b, 0, headerBytes,0,length);
	}

	public int[] getKeys() {
		return keys;
	}

	public void setKeys(int[] keys) {
		this.keys = keys;
		this.keyLength=keys.length;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(keyLength);
		for(int i=0;i<keyLength;i++){
			out.writeInt(keys[i]);
		}
		out.writeInt(length);
		out.write(headerBytes);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		keyLength=in.readInt();
		keys=new int[keyLength];
		for(int i=0;i<keyLength;i++){
			keys[i]=in.readInt();
		}
		length=in.readInt();
		headerBytes=new byte[length];
		in.readFully(headerBytes);
		
	}

	@Override
	public int compareTo(TraceHeaderWritable o) {
		int[] otherKeys=o.getKeys();
		if(otherKeys.length == keys.length){
			for(int i=0;i<keys.length;i++){
				int temp=keys[i]-otherKeys[i];
				if(temp != 0){
					return temp;
				}
			}
		}else{
			throw new RuntimeException("can't compare trace with difference sorted key.");
		}
		
		return 0;
	}

	@Override
	public String toString() {
		return "[keys=" + Arrays.toString(keys) + ", keyLength=" + keyLength + "]";
	}

	
	

}
