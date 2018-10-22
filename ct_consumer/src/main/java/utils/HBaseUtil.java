package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeSet;

public class HBaseUtil {
	/**
	 * 判断表是否存在
	 *
	 * @param conf      HBaseConfiguration
	 * @param tableName
	 * @return
	 */
	public static boolean isExistTable(Configuration conf, String tableName) throws IOException {
		Connection connection = ConnectionFactory.createConnection (conf);
		Admin admin = connection.getAdmin ();
		boolean result = admin.tableExists (TableName.valueOf (tableName));

		admin.close ();
		connection.close ();
		return result;
	}

	/**
	 * 初始化命名空间
	 *
	 * @param conf
	 * @param namespace
	 */
	public static void initNamespace(Configuration conf, String namespace) throws IOException {
		Connection connection = ConnectionFactory.createConnection (conf);
		Admin admin = connection.getAdmin ();

		NamespaceDescriptor nd = NamespaceDescriptor
				.create (namespace)
				.addConfiguration ("Author", "Adrian")
				.addConfiguration ("create_time", String.valueOf (System.currentTimeMillis ()))
				.build ();

		admin.createNamespace (nd);

		admin.close ();
		connection.close ();
	}

	/**
	 * 创建表，完成预分区操作
	 *
	 * @param conf
	 * @param tableName
	 * @param regionsNum
	 * @param columnFamily
	 */
	public static void createTable(Configuration conf, String tableName, int regionsNum, String... columnFamily) throws IOException {
		Connection connection = ConnectionFactory.createConnection (conf);
		Admin admin = connection.getAdmin ();

		if (isExistTable (conf, tableName)) return;

		HTableDescriptor htd = new HTableDescriptor (TableName.valueOf (tableName));
		for (String cf : columnFamily) {
			htd.addFamily (new HColumnDescriptor (cf));
		}

		/**创建 CoProcessor*/
		htd.addCoprocessor ("hbase.CalleeWriteObserver");

		//00|,01|,02 |,03|,04|,05| 的region分区表  getSplitKeys (regionsNum)
		admin.createTable (htd, getSplitKeys (regionsNum));

		admin.close ();
		connection.close ();
	}

	/**
	 * 生成region分区键 00|,01|,02|,03|,04|,05|
	 *
	 * @param regionsNum the number of regions,the last one is idle
	 * @return
	 */
	private static byte[][] getSplitKeys(int regionsNum) {
		//定义一个存放分区的数组
		String[] keys = new String[regionsNum];
		DecimalFormat df = new DecimalFormat ("00");
		for (int i = 0; i < regionsNum; i++) {
			keys[i] = df.format (i) + "|";
		}

		//treeSet 使keys有序
		TreeSet<byte[]> treeSet = new TreeSet<> (Bytes.BYTES_COMPARATOR);
		for (int i = 0; i < regionsNum; i++) {
			treeSet.add (Bytes.toBytes (keys[i]));
		}

		byte[][] splitKeys = new byte[regionsNum][];
		int index = 0;

		Iterator<byte[]> iterator = treeSet.iterator ();
		while (iterator.hasNext ()) {
			byte[] bytes = iterator.next ();
			splitKeys[index++] = bytes;
		}
		//this equals the previous
//		for (byte[] bytes : treeSet) {
//			splitKeys[index++]=bytes;
//		}

		//00|,01|,02|,03|,04|,05|
		return splitKeys;
	}

	//以上是建表，下面是放rowkey等数据

	/**
	 * 生成rowkey
	 * regionCode_call1_buildTime_call2_flag_duration
	 *
	 * @param regionCode 00|,01|,02|,03|,04|,05|
	 * @return
	 */
	public static String getRowKey(String regionCode, String call1, String buildTime, String call2, String flag, String duration) {
		StringBuilder sb = new StringBuilder ();
		sb.append (regionCode + "_")
				.append (call1 + "_")
				.append (buildTime + "_")
				.append (call2 + "_")
				.append (flag + "_")
				.append (duration);

		return sb.toString ();
	}

	/**
	 * 获得前两位 分区号
	 * 通话建立时间：2017-01-10 11:20:30 -> 20170110112030
	 *
	 * @param call1
	 * @param buildTime
	 * @param regionsNum
	 * @return
	 */
	public static String genRegionCode(String call1, String buildTime, int regionsNum) {
		int len = call1.length ();
		//取出后4位号码
		String lastPhone = call1.substring (len - 4);
		//取出年月
		String ym = buildTime
				.replaceAll ("-", "")
				.replaceAll (":", "")
				.replaceAll (" ", "")
				.substring (0, 6);
		//离散操作1  ^ same 0 dif 1
		Integer x = Integer.valueOf (lastPhone) ^ Integer.valueOf (ym);
		//离散操作2
		int y = x.hashCode ();
		//生成分区号
		int regionCode = y % regionsNum;
		//格式化分区号
		DecimalFormat df = new DecimalFormat ("00");
		return df.format (regionCode);
		/*//分区数
		int regions = Integer.parseInt(PropertiesUtil.getProp("hbase.regions"));
		//获取年月
		String yearMonth = buildTime.replaceAll("-", "").substring(0, 6);
		//分区号的生成
		int splitNum = (Integer.parseInt(phoneNum.substring(3, 7)) ^ Integer.parseInt(yearMonth)) % regions;
		return new DecimalFormat("00").format(splitNum);*/
	}
}
