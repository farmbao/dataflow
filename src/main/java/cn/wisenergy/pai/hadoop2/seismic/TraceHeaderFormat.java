package cn.wisenergy.pai.hadoop2.seismic;

import java.util.ArrayList;
import java.util.List;

/**
 * This file stores the header key offset in the internal format
 */
public class TraceHeaderFormat {
	public static final int m_OriginalFieldRecordNo = 0;
	public static final int m_TraceNoWithinOriginalField = 4;
	//public static final int m_ShotNo = 8;
	public static final int m_InLineNo = 12;
	public static final int m_CrossLineNo = 16;
	public static final int m_CDPStationNo = 20;
	public static final int m_CDPNo = 24;
	public static final int m_Offset = 32;
	public static final int m_NumOfSamples = 224;
	public static final int m_DelectorStationNo = 184;
	public static final int m_TraceNo = 28;
	
	public static final int m_CDPX = 132;
	public static final int m_CDPY = 136;

	public static final int m_ScalarOffset = 392;
	public static final int m_ScalarPoint = 394; 
	public static final int m_ScalarCoor = 270;
	
	//容差新曾字段
	public static final int m_PrimarySortBin = 408;
	public static final int m_SecondarySortBin = 412;
	public static final int m_ThirdSortBin = 417;
	
	//notes: paisort used last 16 bits 
	public static final int m_NewTracePos = TraceHeader.FIXED_TRACE_HEADER_LENGTH - 8 * 2;
	public static final int m_oldTracePos = TraceHeader.FIXED_TRACE_HEADER_LENGTH - 8;
	
	private static List<Integer> sortBinPosList = null;
	
	static{
		sortBinPosList = new ArrayList<Integer>();
		sortBinPosList.add(m_PrimarySortBin);
		sortBinPosList.add(m_SecondarySortBin);
		sortBinPosList.add(m_ThirdSortBin);
	}
	
	public static final List<Integer> getSortBinPosList()
	{
		return sortBinPosList;
	}
}

