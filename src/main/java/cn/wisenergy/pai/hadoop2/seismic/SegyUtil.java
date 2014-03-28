/**
 * 
 */
package cn.wisenergy.pai.hadoop2.seismic;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * @author lisonglin
 * 
 */
public class SegyUtil {
	private static final Log LOG = LogFactory.getLog(SegyUtil.class.getName());
	/**
	 * Reverse the bytes.
	 * 
	 * @param str
	 * @param startOffset
	 * @param len
	 * @param big
	 * @return
	 */
	private byte[] ReverseEndian(byte str[], int startOffset, int len, boolean big) {
		byte res[] = new byte[len];
		if (big) {
			for (int i = 0; i < len; i++) {
				res[i] = str[i + startOffset];
			}

		} else {
			for (int i = 0; i < len; i++) {
				res[len - i - 1] = str[i + startOffset];
			}
		}
		return res;

	}

	/**
	 * Convert short to bytes.
	 * 
	 * @param b
	 * @param s
	 * @param index
	 */
	private void Short2Bytes(byte b[], short s, int index) {
		b[index] = (byte) (s >> 8);
		b[index + 1] = (byte) (s >> 0);
	}

	/**
	 * Convert bytes to short.
	 * 
	 * @param b
	 * @param index
	 * @return
	 */
	private short Byte2Short(byte[] b, int index) {
		return (short) (((b[index] << 8) | b[index + 1] & 0xff));
	}

	// ///////////////////////////////////////////////////////
	/**
	 * Convert int to bytes.
	 */
	public static void Int2Bytes(byte[] bb, int x, int index) {
		bb[index + 0] = (byte) (x >> 24);
		bb[index + 1] = (byte) (x >> 16);
		bb[index + 2] = (byte) (x >> 8);
		bb[index + 3] = (byte) (x >> 0);
	}

	/**
	 * Convert bytes to int.
	 * 
	 * @param bb
	 * @param index
	 * @return
	 */
	private int Byte2Int(byte[] bb, int index) {
		return (int) ((((bb[index + 0] & 0xff) << 24) | ((bb[index + 1] & 0xff) << 16) | ((bb[index + 2] & 0xff) << 8) | ((bb[index + 3] & 0xff) << 0)));
	}

	// /////////////////////////////////////////////////////////
	/**
	 * Convert long to bytes.
	 */
	public static void Long2Bytes(byte[] bb, long x, int index) {
		bb[index + 0] = (byte) (x >> 56);
		bb[index + 1] = (byte) (x >> 48);
		bb[index + 2] = (byte) (x >> 40);
		bb[index + 3] = (byte) (x >> 32);
		bb[index + 4] = (byte) (x >> 24);
		bb[index + 5] = (byte) (x >> 16);
		bb[index + 6] = (byte) (x >> 8);
		bb[index + 7] = (byte) (x >> 0);
	}

	/**
	 * Convert bytes to long.
	 * 
	 * @param bb
	 * @param index
	 * @return
	 */
	private long Byte2Long(byte[] bb, int index) {
		return ((((long) bb[index + 0] & 0xff) << 56) | (((long) bb[index + 1] & 0xff) << 48)
				| (((long) bb[index + 2] & 0xff) << 40) | (((long) bb[index + 3] & 0xff) << 32)
				| (((long) bb[index + 4] & 0xff) << 24) | (((long) bb[index + 5] & 0xff) << 16)
				| (((long) bb[index + 6] & 0xff) << 8) | (((long) bb[index + 7] & 0xff) << 0));
	}

	/**
	 * Read int value.
	 * 
	 * @return
	 */
	public int ReadInt() {
		curOffset += 4;
		return readInt(curOffset - 4);
	}

	public int readInt(int start) {
		return Byte2Int(ReverseEndian(res, start, 4, this.bigEndian), 0);
	}

	public short readShort(int start) {
		return Byte2Short(ReverseEndian(res, start, 2, this.bigEndian), 0);
	}
	
	public long readLong(int start) {
		return Byte2Long(ReverseEndian(res, start, 8, this.bigEndian), 0);
	}

	/**
	 * Read short value.
	 * 
	 * @return
	 */
	public short ReadShort() {
		curOffset += 2;
		return Byte2Short(ReverseEndian(this.res, this.curOffset - 2, 2, this.bigEndian), 0);
	}

	/**
	 * Read float value.
	 * 
	 * @return
	 */
	public float ReadFloat() {
		curOffset += 4;
		return Float.intBitsToFloat(Byte2Int(ReverseEndian(res, curOffset-4, 4, this.bigEndian), 0));
//		return Byte2Float(ReverseEndian(this.res, this.curOffset - 4, 4, this.bigEndian));
	}

	public long ReadLong() {
		curOffset += 8;
		return Byte2Long(ReverseEndian(this.res, this.curOffset - 8, 8, this.bigEndian), 0);
	}
	
	public double ReadDouble() {
		curOffset += 8;
		return Byte2Double(ReverseEndian(this.res, this.curOffset - 8, 8, this.bigEndian));
	}
	
	/**
	 * Convert float to bytes.
	 * 
	 * @param v
	 * @return
	 */
	private byte[] Float2Byte(float v) {
		ByteBuffer bb = ByteBuffer.allocate(4);
		byte[] ret = new byte[4];
		FloatBuffer fb = bb.asFloatBuffer();
		fb.put(v);
		bb.get(ret);
		return ret;
	}

	/**
	 * Convert bytes to float.
	 * 
	 * @param v
	 * @return
	 */
	private float Byte2Float(byte[] v) {
		ByteBuffer bb = ByteBuffer.wrap(v);
		FloatBuffer fb = bb.asFloatBuffer();
		return fb.get();
	}
	
	public static byte[] floatReverseBytes(float f){
		ByteBuffer bb = ByteBuffer.allocate(4);
		byte[] ret = new byte[4];
		FloatBuffer fb = bb.asFloatBuffer();
		fb.put(f);
		bb.get(ret);
		byte r0 = ret[0];
		byte r1 = ret[1];
		byte r2 = ret[2];
		byte r3 = ret[3];
		ret[0] = r3;
		ret[1] = r2;
		ret[2] = r1;
		ret[3] = r0;
		return ret;
	}
	
	public static void writeFloat(DataOutputStream out,float f) throws IOException{
		byte[] bytes2 = floatReverseBytes(f);
		for(int i=0;i<bytes2.length;i++){
			out.writeByte(bytes2[i]);
		}
	}
	
	/**
	 * Convert bytes to double.
	 * 
	 * @param v
	 * @return
	 */
	private double Byte2Double(byte[] v) {
		ByteBuffer bb = ByteBuffer.wrap(v);
		DoubleBuffer db = bb.asDoubleBuffer();
		return db.get();
	}
	

	private byte[] res = null;
	private boolean bigEndian = false;
	private int curOffset = 0;

	/**
	 * Constructor.
	 */
	public SegyUtil(byte[] res, boolean bigEndian) {
		this.res = res;
		this.bigEndian = bigEndian;
	}

	/**
	 * For internal test only.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		byte[] b = new byte[8];
		SegyUtil s = new SegyUtil(b, false);
		SegyUtil.Long2Bytes(b, 15l, 0);
		LOG.info(s.Byte2Long(b, 0));
	}

	public static int readInt(byte[] b, int index) {
		byte bb[] = new byte[4];
		for (int i = 0; i < 4; i++) {
			bb[4 - 1 - i] = b[i + index];
		}
		return (int) ((((bb[0] & 0xff) << 24) | ((bb[1] & 0xff) << 16) | ((bb[2] & 0xff) << 8) | ((bb[3] & 0xff) << 0)));
	}

	public static long readLong(byte[] b, int index) {
		byte bb[] = new byte[8];
		for (int i = 0; i < 8; i++) {
			bb[8 - 1 - i] = b[i + index];
		}
		return ((((long) bb[0] & 0xff) << 56) | (((long) bb[1] & 0xff) << 48) | (((long) bb[2] & 0xff) << 40)
				| (((long) bb[3] & 0xff) << 32) | (((long) bb[4] & 0xff) << 24) | (((long) bb[5] & 0xff) << 16)
				| (((long) bb[6] & 0xff) << 8) | (((long) bb[7] & 0xff) << 0));
	}
}
