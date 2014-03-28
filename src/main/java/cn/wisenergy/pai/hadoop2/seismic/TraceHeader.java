package cn.wisenergy.pai.hadoop2.seismic;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * @class IndexTraceHeader
 * @brief 道头定义
 */
public class TraceHeader {
	private static final Log LOG = LogFactory.getLog(TraceHeader.class);
	public final static int FIXED_TRACE_HEADER_LENGTH = 1024; // Fixed length.

	byte[] buf = new byte[FIXED_TRACE_HEADER_LENGTH];

	// 字段编号 1 位置 1-4 对应segy 位置 9-12 野外原始记录号（炮集号）
	public int m_OriginalFieldRecordNo;

	// 字段编号 2 位置 5-8 对应segy 位置 13-16 原名：m_TraceNoWithin 野外原始记录的道号（炮集中的道序号）
	public int m_TraceNoWithinOriginalField;

	// 字段编号 3 位置 9-12 对应segy 位置 17-20 原名：m_EnergySourcePtNo
	// 震源点号――当在相同有效地表位置多于一个记录时使用
	public int m_ShotNo;

	// 字段编号 4 位置 13-16 对应segy 位置 189-192 对于3-D 数据，本字段用来填纵向线（in-line）。 若每个SEGY
	// 文件记录一条纵向线，文件中所有道的该值应相同， 并且同样的值将记录在二进制文件头的3205-3206字节中
	public int m_InLineNo;

	// 字段编号 5 位置 17-20 对应segy 位置 193-196 对于3-D叠后数据，本字段用来填横向线号（Cross-line）。
	// 它应与道头21-24字节中的道集（CDP）号的值一致， 但这并不是实例。
	public int m_CrossLineNo;

	// 字段编号 6 位置 21-24 对应segy 位置 无 道集（CDP）站号
	public int m_CDPStationNo;

	// 字段编号 7 位置 25-28 对应segy 位置 21-24 原名：m_EnsembleNo 道集号（即CDP，CMP，CRP等）
	public int m_CDPNo;

	// 字段编号 8 位置 29-32 对应segy 位置 25-28 原名：m_TraceNoWithinEnsemble
	// 道集的道数――每个道集从道号1开始
	public int m_TraceNo;

	// 字段编号 9 位置 33-36 对应segy 位置 37-40 从震源中心点到检波器组中心的距离（若与炮激发线方向相反取负）
	public int m_Offset;

	// 字段编号 10 位置 37-40 对应segy 位置 45-48 震源地表高程
	public int m_SurfaceElevation;

	// 字段编号 11 位置 41-44 对应segy 位置 41-44 检波器组高程（所有基准以上高程为正，以下为负）
	public int m_ReceiverGrpElevation;

	// 字段编号 12 位置 45-48 对应segy 位置 无 CDP高程
	public int m_CDPElevation;

	// 字段编号 13 位置 49-52 对应segy 位置 57-60 震源基准高程
	public int m_DatumElevationAtSrc;

	// 字段编号 14 位置 53-56 对应segy 位置 53-56 检波器组基准高程
	public int m_DatumElevationAtRec;

	// 字段编号 15 位置 57-60 对应segy 位置 无 CDP 浮动基准面的高程
	public int m_CDPFloatElevation;

	// 字段编号 16 位置 61-64 对应segy 位置 49-52 震源距地表深度（正数）
	public int m_SourceDepth;

	// 字段编号 17 位置 65-68 对应segy 位置 61-64 震源水深
	public int m_WaterDepthAtSrc;

	// 字段编号 18 位置 69-72 对应segy 位置 65-68 检波器组水深
	public int m_WaterDepthAtRcvGrp;

	// 字段编号 19 位置 73-76 对应segy 位置 95-96 震源处井口时间（毫秒）
	public int m_UpholeTimeAtSrc;

	// 字段编号 20 位置 77-80 对应segy 位置 97-98 检波器组处井口时间（毫秒）
	public int m_UpholeTimeAtGrp;

	// 字段编号 21 位置 81-84 对应segy 位置 91-92 风化层速度（如二进制文件头3255-3256字节指明的ft/s或m/s）
	public int m_WeaVelocity;

	// 字段编号 22 位置 85-88 对应segy 位置 93-94 风化层下速度（如二进制文件头3255-3256字节指明的ft/s或m/s）
	public int m_SubWeaVelocity;

	// 字段编号 23 位置 89-92 对应segy 位置 99-100 震源的静校正量（毫秒）
	public int m_SrcStaticCorrection;

	// 字段编号 24 位置 93-96 对应segy 位置 101-102 检波器组的校正量（毫秒）
	public int m_GrpStaticCorrection;

	// 字段编号 25 位置 97-100 对应segy 位置 103-104 应用的总静校正量（毫秒）（如没有应用静校正量为零）
	public int m_TotalStaticApplied;

	// 字段编号 26 位置 101-104 对应segy 位置 无 地震道低频分量
	public int m_TraceStaticToCMP;

	// 字段编号 27 位置 105-108 对应segy 位置 无 已经应用的地震道低频分量
	public int m_TraceStaticToCMPApp;

	// 字段编号 28 位置 109-112 对应segy 位置 无 检波点高频静校正量
	public int m_TraceStaticToHiReceiver;

	// 字段编号 29 位置 113-116 对应segy 位置 无 已经应用的检波点高频静校正量
	public int m_TraceStaticToHiReceiverApp;

	// 字段编号 30 位置 117-120 对应segy 位置 无 炮点高频静校正量
	public int m_TraceStaticToHiSource;

	// 字段编号 31 位置 121-124 对应segy 位置 无 已经应用的炮点高频静校正量
	public int m_TraceStaticToHiSourceApp;

	// 字段编号 32 位置 125-128 对应segy 位置 73-76 震源坐标――X
	public int m_SrcCoordinateX;

	// 字段编号 33 位置 129-132 对应segy 位置 77-80 震源坐标――Y
	public int m_SrcCoordinateY;

	// 字段编号 34 位置 133-136 对应segy 位置 81-84 检波器组坐标――X
	public int m_GrpCoordinateX;

	// 字段编号 35 位置 137-140 对应segy 位置 85-88 检波器组坐标――Y
	public int m_GrpCoordinateY;

	// 字段编号 36 位置 141-144 对应segy 位置 181-184 该道的道集（CDP）位置X坐标（应用道头71-72字节的因子）
	public int m_CDPX;

	// 字段编号 37 位置 145-148 对应segy 位置 185-188 该道的道集（CDP）位置Y坐标（应用道头71-72字节的因子）
	public int m_CDPY;

	// 字段编号 38 位置 149-152 对应segy 位置 无 炮点线号
	public int m_SourceLineName;

	// 字段编号 39 位置 153-156 对应segy 位置 无 炮点桩号
	public int m_SourcePointNo;

	// 字段编号 40 位置 157-160 对应segy 位置 无 炮点站号 （线号+桩号）
	public int m_SourceStationNo;

	// 字段编号 41 位置 161-164 对应segy 位置 无 炮点索引号
	public int m_SourcePointIndex;

	// 字段编号 42 位置 165-168 对应segy 位置 无 炮点代码类型
	public int m_SourcePointCode;

	// 字段编号 43 位置 169-172 对应segy 位置 无 炮点野外静矫正量
	public int m_SourceFieldStaticCorrection;

	// 字段编号 44 位置 173-176 对应segy 位置 无 炮点时间
	public int m_SourceTime;

	// 字段编号 45 位置 177-180 对应segy 位置 无 检波点线号
	public int m_DelectorLineName;

	// 字段编号 46 位置 181-184 对应segy 位置 无 检波点桩号
	public int m_DelectorPointNo;

	// 字段编号 47 位置 185-188 对应segy 位置 无 检波点站号（线号+桩号）
	public int m_DelectorStationNo;

	// 字段编号 48 位置 189-192 对应segy 位置 无 检波点索引号
	public int m_DelectorPointIndex;

	// 字段编号 49 位置 193-196 对应segy 位置 无 检波点代码类型
	public int m_DelectorPointCode;

	// 字段编号 50 位置 197-200 对应segy 位置 无 检波点野外静矫正量
	public int m_DelectorFieldStaticCorrection;

	// 字段编号 51 位置 201-204 对应segy 位置 无 检波点时间
	public int m_DelectorTime;

	// 字段编号 52 位置 205-208 对应segy 位置 无 野外磁带号
	public int m_CrossRelationFieldTapeNo;

	// 字段编号 53 位置 209-212 对应segy 位置 无 野外记录号增量
	public int m_CrossRelationFieldRecodeIncrement;

	// 字段编号 54 位置 213-216 对应segy 位置 无 仪器代码
	public int m_CrossRelationInstrumentCode;

	// 字段编号 55 位置 217-220 对应segy 位置 无 最大采样振幅（Amp）值
	public int m_MaxSampleValue;

	// 字段编号 56 位置 221-224 对应segy 位置 无 最小采样振幅（Amp）值
	public int m_MinSampleValue;

	// 字段编号 57 位置 225-228 对应segy 位置 115-116 该道采样点数
	public int m_NumOfSamples;

	// 字段编号 58 位置 229-232 对应segy 位置 117-118 该道采样间隔（微秒）
	public int m_SampleInterval;

	// 字段编号 59 位置 233-236 对应segy 位置 无 排列旗标，是中间炮还是单边炮
	public int m_SpreadFlag;

	// 字段编号 60 位置 237-240 对应segy 位置 无 线间距（3D InLine）
	public int m_LineInterval;

	// 字段编号 61 位置 241-244 对应segy 位置 无 道间距
	public int m_TraceInterval;

	// 字段编号 62 位置 245-248 对应segy 位置 111-112 起始切除时间（毫秒） （第一个非零样点时间）
	public int m_MuteTimeS;

	// 字段编号 63 位置 249-252 对应segy 位置 113-114 终止切除时间（毫秒） （最后一个非零样点时间）
	public int m_MuteTimeE;

	// 字段编号 64 位置 253-254 对应segy 位置 31-32 产生该道的垂直叠加道数。（1是一道，2是两道求和，…）
	public short m_NumOfVSumTraces;

	// 字段编号 65 位置 255-256 对应segy 位置 33-34 产生该道的水平叠加道数。（1是一道，2是两道求和，…）
	public short m_NumOfHStackedTraces;

	// 字段编号 66 位置 257-260 对应segy 位置 105-106 延迟时间A――以毫秒表示的240字节道识别头的结束和时间断点之间的时间。
	// 当时间断点出现在头之后，该值为正； 当时间断点出现在头之前，该值为负。 时间断点是最初脉冲，它由辅助道记录或由其他记录系统指定。
	public int m_LagTimeA;

	// 字段编号 67 位置 261-264 对应segy 位置 107-108 延迟时间B――以毫秒表示的时间断点到能量源起爆时间之间的时间。可正可负。
	public int m_LagTimeB;

	// 字段编号 68 位置 265-268 对应segy 位置 109-110 记录延迟时间――以毫秒表示的能量源起爆时间到数据采样开始记录之间的时间。
	public int m_DelayRecordingTime;

	// 字段编号 69 位置 269-270 对应segy 位置 69-70 应用于所有在道头41-68字节给定的真实高程和深度的因子。
	// 因子＝1，＋10，＋100，或＋1000 若为正，因子为乘数 若为负，因子为除数
	public short m_ScalarElev;

	// 字段编号 70 位置 271-272 对应segy 位置 71-72 应用于所有在道头73-88字节和181-188字节给定的真实坐标值的因子。
	// 因子＝1，＋10，＋100，或＋1000 若为正，因子为乘数 若为负，因子为除数
	public short m_ScalarCoor;
	
	// 字段编号 71 位置 273-274 对应segy 位置 69-70 应用于NO21:m_WeaVelocity NO22:m_SubWeaVelocity的速度值的因子。
	// 因子＝1，＋10，＋100，或＋1000 若为正，因子为乘数 若为负，因子为除数
	public short m_ScalarSpeed;

	// 字段编号 72 位置 275-276 对应segy 位置 71-72 应用于NO:55 m_MaxSampleValue NO:56 m_MinSampleValue 采样值的因子。
	// 因子＝1，＋10，＋100，或＋1000 若为正，因子为乘数 若为负，因子为除数
	public short m_ScalarAmp;

	// 字段编号 73 位置 277-280 对应segy 位置 89-90 坐标单位： 1＝长度（米或英尺） 2＝弧度秒 3＝小数度
	// 4＝度，分，秒（DMS）
	public int m_CoordinateUnit;

	// 字段编号 74 位置 281-284 对应segy 位置 197-200 原名：m_ShotNo 炮点号――这可能只应用于2-D叠后数据
	public int m_ShotNoNewEntry;

	// 字段编号 75 位置 285-288 对应segy 位置 201-204 原名：m_ScalarShotNo
	// 应用于道头中197-200字节中炮点号的因子，以得到实际数值。 若为正，因子用作乘数； 若为负，因子用作除数；
	// 若为零,炮点号不用于因子作用（即它是一个整数。典型的值是-10，允许炮点号小数点后有一位小数）。
	public int m_ScalarNoNewEntry;

	// 字段编号 76 位置 289-292 对应segy 位置 1-4 测线中道顺序号――若一条测线有若干SEG Y文件号数递增
	public int m_TraceSeqNoWithLine;

	// 字段编号 77 位置 293-296 对应segy 位置 5-8 SEG Y文件中道顺序号――每个文件以道顺序1开始
	public int m_TraceSeqNoWithInFile;

	// 字段编号 78 位置 297-298 对应segy 位置 29-30 道识别码： -1=其他 0＝未知 1＝地震数据 2＝死道 3＝哑道 4＝时断
	// 5＝井口 6＝扫描 7＝定时 8＝水断 9＝近场枪信号 10＝远场枪信号 11＝地震压力传感器 12＝多分量地震传感器――垂直分量
	// 13＝多分量地震传感器――横向分量 14＝多分量地震传感器――纵向分量 15＝旋转多分量地震传感器――垂直分量
	// 16＝旋转多分量地震传感器――切向分量 17＝旋转多分量地震传感器――径向分量 18＝可控源反应质量 19＝可控源底盘 20＝可控源估计地面力
	// 21＝可控源参考 22＝时间速度对 23…N＝选用，（最大N＝32767）
	public short m_TraceId;

	// 字段编号 79 位置 299-300 对应segy 位置 35-36 数据用途： 1＝生产 2＝试验
	public short m_DataUse;

	// 字段编号 80 位置 301-302 对应segy 位置 119-120 野外仪器增益类型： 1＝固定 2＝二进制 3＝浮点 4…N＝选用
	public short m_Gain;

	// 字段编号 81 位置 303-304 对应segy 位置 121-122 仪器增益常数（分贝）
	public short m_InstrumentGain;

	// 字段编号 82 位置 305-306 对应segy 位置 123-124 仪器初始增益（分贝）
	public short m_InstrumentEOrInitGain;

	// 字段编号 83 位置 307-308 对应segy 位置 125-126 相关：1＝无 2＝有
	public short m_Correlated;

	// 字段编号 84 位置 309-310 对应segy 位置 127-128 起始扫描频率（赫兹）
	public short m_SweepFreqAtS;

	// 字段编号 85 位置 311-312 对应segy 位置 129-130 终止扫描频率（赫兹）
	public short m_SweepFreqAtE;

	// 字段编号 86 位置 313-314 对应segy 位置 131-132 扫描长度（毫秒）
	public short m_SweepLen;

	// 字段编号 87 位置 315-316 对应segy 位置 133-134 扫描类型： 1＝线性 2＝抛物线 3＝指数 4＝其他
	public short m_SweepType;

	// 字段编号 88 位置 317-318 对应segy 位置 135-136 扫描道斜坡起始长度（毫秒）
	public short m_SweepTraceTaperLenAtS;

	// 字段编号 89 位置 319-320 对应segy 位置 137-138 扫描道斜坡终止长度（毫秒）
	public short m_SweepTraceTaperLenAtE;

	// 字段编号 90 位置 321-322 对应segy 位置 139-140 斜坡类型：1＝线性 2＝cos2 3＝其他
	public short m_TaperType;

	// 字段编号 91 位置 323-324 对应segy 位置 141-142 假频滤波频率（赫兹），若使用
	public short m_AliasFilterFreq;

	// 字段编号 92 位置 325-326 对应segy 位置 143-144 假频滤波坡度（分贝/倍频程）
	public short m_AliasFilterSlope;

	// 字段编号 93 位置 327-328 对应segy 位置 145-146 陷波频率（赫兹），若使用
	public short m_NotchFilterFreq;

	// 字段编号 94 位置 329-330 对应segy 位置 147-148 陷波坡度（分贝/倍频程）
	public short m_NotchFilterSlope;

	// 字段编号 95 位置 331-332 对应segy 位置 149-150 低截频率（赫兹），若使用
	public short m_LowCutFreq;

	// 字段编号 96 位置 333-334 对应segy 位置 151-152 高截频率（赫兹），若使用
	public short m_HighCutFreq;

	// 字段编号 97 位置 335-336 对应segy 位置 153-154 低截坡度（分贝/倍频程）
	public short m_LowCutSlope;

	// 字段编号 98 位置 337-338 对应segy 位置 155-156 高截坡度（分贝/倍频程）
	public short m_HighCutSlope;

	// 字段编号 99 位置 339-340 对应segy 位置 157-158 数据记录的年
	public short m_Year;

	// 字段编号 100 位置 341-342 对应segy 位置 159-160 日（以格林尼治标准时间和通用协调时间为基准的公元日）
	public short m_Day;

	// 字段编号 101 位置 343-344 对应segy 位置 161-162 时（24小时制）
	public short m_Hour;

	// 字段编号 102 位置 345-346 对应segy 位置 163-164 分
	public short m_Minute;

	// 字段编号 103 位置 347-348 对应segy 位置 165-166 秒
	public short m_Second;

	// 字段编号 104 位置 349-350 对应segy 位置 167-168 时间基准码： 1＝当地 2＝格林尼治标准时间
	// 3＝其他，应在扩展原文文件头的用户定义文本段解释 4＝通用协调时间
	public short m_TimBas;

	// 字段编号 105 位置 351-352 对应segy 位置 169-170
	// 道加权因子――最小有效位数定义为2-N伏。（N＝0，1，…，32767）
	public short m_TraceWeightFactor;

	// 字段编号 106 位置 353-354 对应segy 位置 171-172 滚动开关位置1的检波器组号
	public short m_GeoGrpNoOfRoll;

	// 字段编号 107 位置 355-356 对应segy 位置 173-174 野外原始记录中道号1的检波器组号
	public short m_GeoGrpNoOfTrace;

	// 字段编号 108 位置 357-358 对应segy 位置 175-176 野外原始记录中最后一道的检波器组号
	public short m_GeoGrpNoOfLastTrace;

	// 字段编号 109 位置 359-360 对应segy 位置 177-178 间隔大小（滚动时甩掉的总检波器组数）
	public short m_GapSize;

	// 字段编号 110 位置 361-362 对应segy 位置 179-180 相对测线斜坡起始或终止点的移动 1＝下（或后） 2＝上（或前）
	public short m_OverTravel;

	// 字段编号 111 位置 363-364 对应segy 位置 203-204 道值测量单位： -1＝其他（应在数据采样测量单位文本段描述） 0＝未知
	// 1＝帕斯卡（Pa） 2＝伏特（V） 3＝毫伏（mV） 4＝安培（A） 5＝米（m） 6＝米每秒（m/s） 7＝米每秒二次方（m/s2）
	// 8＝牛顿（N） 9＝瓦特（W）
	public short m_TraceValueMeasurement;

	// 字段编号 112 位置 365-368 对应segy 位置 205-208
	// 转换常数――该倍数用于将数据道采样转换成转换单位（道头211-212字节指定）。
	// 本常数以4字节编码，尾数是两互补整数（205-208字节）和2字节，
	// 十的指数幂是两互补整数（209-210字节）（即（205-208字节）×10**（209-210字节））
	public int m_TransConstMantissa;

	// 字段编号 113 位置 369-370 对应segy 位置 209-210
	// 转换常数――该倍数用于将数据道采样转换成转换单位（道头211-212字节指定）。
	// 本常数以4字节编码，尾数是两互补整数（205-208字节）和2字节，
	// 十的指数幂是两互补整数（209-210字节）（即（205-208字节）×10**（209-210字节））
	public short m_TransConstPow;

	// 字段编号 114 位置 371-372 对应segy 位置 211-212
	// 转换单位――经乘以道头205-210字节中的转换常数后的数据道采样测量单位。 -1＝其他（应在数据采样测量单位文本段36页描述） 0＝未知
	// 1＝帕斯卡（Pa） 2＝伏特（V） 3＝毫伏（mV） 4＝安培（A） 5＝米（m） 6＝米每秒（m/s） 7＝米每秒二次方（m/s2）
	// 8＝牛顿（N） 9＝瓦特（W）
	public short m_TransductionUnit;

	// 字段编号 115 位置 373-374 对应segy 位置 213-214 设备/道标识――与数据道关联的单位号或设备号
	public short m_Indentifier;

	// 字段编号 116 位置 375-376 对应segy 位置 215-216
	// 在道头95-114字节给出的作用于时间的因子，以得到真实的毫秒表示的时间值。 因子＝1，＋10，＋100，＋1000或＋10000。
	// 若为正，因子用作乘数； 若为负，因子用作除数。 为零设定因子为一。
	public short m_ScalarTime;

	// 字段编号 117 位置 377-378 对应segy 位置 217-218 震源类型/方位――定义类型或能量源的方位。 -1到-n＝其他 0＝未知
	// 1＝可控震源――垂直方位 2＝可控震源――横向方位 3＝可控震源――纵向方位 4＝冲击源――垂直方位 5＝冲击源――横向方位
	// 6＝冲击源――纵向方位 7＝分布式冲击源――垂直方位 8＝分布式冲击源――横向方位 9＝分布式冲击源――纵向方位
	public short m_SrcType;

	// 字段编号 118 位置 379-380 对应segy 位置 219-220 对震源方位的震源能量方向――正方位方向在道头217-218字节定义。
	// 能量方向以度数长度编码（即347.8°编码成3478）
	public short m_OrientationDirection;

	// 字段编号 119 位置 381-384 对应segy 位置 221-224 对震源方位的震源能量方向――正方位方向在道头217-218字节定义。
	// 能量方向以度数长度编码（即347.8°编码成3478）
	public int m_EnergyDirection;

	// 字段编号 120 位置 385-388 对应segy 位置 225-228 震源测量――描述产生道的震源效应。
	// 测量可以简单，定量的测量如使用炸药总重量或气枪压力峰值或可控源振动次数和扫描周期时间。
	// 尽管这些简单的测量可接受，但最好使用真实的能量或工作测量单位。 本常数编码成4字节，尾数为两互补整数（225-228字节）和2字节，
	// 十的指数幂是两互补整数（209-230字节）（即（225-228字节）×10**（229-230字节））。
	public int m_SrcMeasurementMantissa;

	// 字段编号 121 位置 389-390 对应segy 位置 229-230 震源测量――描述产生道的震源效应。
	// 测量可以简单，定量的测量如使用炸药总重量或气枪压力峰值或可控源振动次数和扫描周期时间。
	// 尽管这些简单的测量可接受，但最好使用真实的能量或工作测量单位。 本常数编码成4字节，尾数为两互补整数（225-228字节）和2字节，
	// 十的指数幂是两互补整数（209-230字节）（即（225-228字节）×10**（229-230字节））。
	public short m_SrcMeasurementPow;

	// 字段编号 122 位置 391-392 对应segy 位置 231-232 震源测量单位――用于震源测量,道头225-230字节的单位。
	// -1＝其他（应在震源测量单位文本段39页描述） 0＝未知 1＝焦耳（J） 2＝千瓦（kW） 3＝帕斯卡（Pa） 4＝巴（Bar）
	// 4＝巴－米（Bar-m） 5＝牛顿（N） 6＝千克（kg）
	public short m_SrcMeasurementUnit;

	// 字段编号 123 位置 393-394 对应segy 位置 无 道间距、线间距、CDP 间距、偏移距的因子
	public short m_ScalarOffset;

	// 字段编号 124 位置 395-396 对应segy 位置 无 炮点线号、炮点桩号的因子
	public short m_ScalarPoint;
	
	// 字段编号 125 位置 397-400 对应segy 位置 无 震源检波器中点X 坐标
	public int m_MidPointX;
	
	// 字段编号 126 位置 401-404 对应segy 位置 无 震源检波器中点Y 坐标
	public int m_MidPointY;
	
	// 字段编号 127 位置 405-408 对应segy 位置 无 震源检波器中点Y 坐标
	public int m_PointDepth;
	
	//字段编号 128 位置 409-412 对应segy 位置 无 容差后的 道集排序第一字段值
	public int m_PrimarySortBin;

	//字段编号 129 位置 413-416 对应segy 位置 无 容差后的 道集排序第二字段值
	public int m_SecondarySortBin;

	//字段编号 130 位置 417-420 对应segy 位置 无 容差后的 道集排序第三字段值
	public int m_ThirdSortBin;

	//字段编号 131 位置 421-424 对应segy 位置 无	 炮点剩余静校正量赋值因子 m_ScalarTime    默认： -10
	public int  m_StaticsSourceResid;
	
	//字段编号 132 位置 425-428 对应segy 位置 无	 检波点剩余静校正量	赋值因子 m_ScalarTime    默认： -10
	public int  m_StaticsReceiverResid;
	
	//字段编号 133 位置 429-432 对应segy 位置 无	 炮点剩余静校正量应用 赋值因子 m_ScalarTime    默认： -10
	public int  m_StaticsSourceResidApp;
	
	//字段编号 134 位置 433-436 对应segy 位置 无	 检波点剩余静校正量应用	赋值因子 m_ScalarTime    默认： -10 
	public int  m_StaticsReceiverResidApp;

	// 字段编号 135 位置 437-1024 对应segy 位置 233-240
	public int[] m_unass; // fixed length = [155];

	private long length = 0;
	private int numOfTraces = 0;


	/**
	 * get Instance for header bytes
	 * @param bytes header bytes
	 */
	public static TraceHeader getInstance(byte[] bytes){
		TraceHeader header = new TraceHeader();
		header.setData(bytes);
		return header;
	}
	
	public TraceHeader(){}




	public byte[] getData() {
		return buf;
	}

	/**
	 * Set the value in bytes.
	 * 
	 * @param bytes
	 */
	public void setData(byte[] bytes) {
		SegyUtil util = new SegyUtil(bytes, false);
		//炮号由 m_ShotNo 改为 m_OriginalFieldRecordNo  索引中名仍为 SHOTNO
		m_OriginalFieldRecordNo = util.readInt(TraceHeaderFormat.m_OriginalFieldRecordNo); // 8
		//m_ShotNo = util.readInt(TraceHeaderFormat.m_ShotNo); // 8
		m_TraceNoWithinOriginalField = util
				.readInt(TraceHeaderFormat.m_TraceNoWithinOriginalField); // 4
		m_CDPStationNo = util.readInt(TraceHeaderFormat.m_CDPStationNo); // 20
		m_CDPNo = util.readInt(TraceHeaderFormat.m_CDPNo); // 24
		m_Offset = util.readInt(TraceHeaderFormat.m_Offset); // 32
		m_InLineNo = util.readInt(TraceHeaderFormat.m_InLineNo); // 12
		m_CrossLineNo = util.readInt(TraceHeaderFormat.m_CrossLineNo); // 16
		m_NumOfSamples = util.readInt(TraceHeaderFormat.m_NumOfSamples); // 224
		m_DelectorStationNo = util
				.readInt(TraceHeaderFormat.m_DelectorStationNo); // 184
		m_TraceNo = util.readInt(TraceHeaderFormat.m_TraceNo);//28
		m_CDPX = util.readInt(TraceHeaderFormat.m_CDPX);//132
		m_CDPY = util.readInt(TraceHeaderFormat.m_CDPY);//136
		m_ScalarCoor = util.readShort(TraceHeaderFormat.m_ScalarCoor); //270
		m_ScalarOffset = util.readShort(TraceHeaderFormat.m_ScalarOffset); //392
		m_ScalarPoint = util.readShort(TraceHeaderFormat.m_ScalarPoint); //394
		
		//字段编号 128 位置 409-412 对应segy 位置 无 容差后的 道集排序第一字段值
		m_PrimarySortBin = util.readInt(TraceHeaderFormat.m_PrimarySortBin); // 408
		//字段编号 129 位置 413-416 对应segy 位置 无 容差后的 道集排序第二字段值
		m_SecondarySortBin = util.readInt(TraceHeaderFormat.m_SecondarySortBin); // 412
		//字段编号 130 位置 417-420 对应segy 位置 无 容差后的 道集排序第三字段值
		m_ThirdSortBin = util.readInt(TraceHeaderFormat.m_ThirdSortBin); // 417
	}

	/**
	 * Get the total number of traces in this file.
	 * 
	 * @return
	 */
	public int GetTotalTraceNum() {
		return numOfTraces;
	}


	

	/**
	 * Get the index of the trace in the file.
	 * 
	 * @return
	 */
	public int GetTraceIndex() {
		return m_TraceSeqNoWithInFile;
	}

	/**
	 * Get CMP num.
	 * 
	 * @return
	 */
	public int GetCMP(){
		return m_CDPNo;
	}


	public int GetCMPLine() {
		return m_OriginalFieldRecordNo;
	}

	public int GetOffset() {
		return m_Offset;
	}

	public int getInLineNum() {
		return m_InLineNo;
	}

	public int getXLineNum() {
		return m_CrossLineNo;
	}

	public static String getTraceSeq(String inputName) {
		if (inputName.equalsIgnoreCase("1")) {
			return "m_CrossLineNo";
		}
		if (inputName.equalsIgnoreCase("2")) {
			return "m_InLineNo";
		}
		return "null";
	}
	
	public int getTraceNum(){
		return this.m_TraceNo;
	}

	public int getNumOfSamples() {
		return m_NumOfSamples;
	}

	@Override
	public String toString() {
		return "[ OriginalFieldRecordNo=" + m_OriginalFieldRecordNo
				+ ", TraceNoWithinOriginalField=" + m_TraceNoWithinOriginalField + ", ShotNo=" + m_ShotNo
				+ ", InLineNo=" + m_InLineNo + ", CrossLineNo=" + m_CrossLineNo + ", CDPStationNo="
				+ m_CDPStationNo + ", CDPNo=" + m_CDPNo + ", TraceNo=" + m_TraceNo + ", Offset=" + m_Offset
				+ ", NumOfSamples=" + m_NumOfSamples
			    + "]";
	}
	
}
