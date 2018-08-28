package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.ConnectionInstance;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class HBaseDAO {

	private static Configuration conf;
	private Connection connection;
	private int regionsNum;
	private String namespace;
	private String tableName;
	private HTable table;
	private SimpleDateFormat sdf1 = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sdf2 = new SimpleDateFormat ("yyyyMMddHHmmss");
	/**优化处*/
	private List<Put> cacheList = new ArrayList<> ();

	static {
		conf = HBaseConfiguration.create ();
	}

	/**
	 * hbase.calllog.regions=6
	 * hbase.calllog.namespace=ns_ct
	 * hbase.calllog.tablename=ns_ct:calllog
	 */
	public HBaseDAO() {
		try {
			regionsNum = Integer.parseInt (PropertiesUtil.getProperty ("hbase.calllog.regions"));
			namespace = PropertiesUtil.getProperty ("hbase.calllog.namespace");
			tableName = PropertiesUtil.getProperty ("hbase.calllog.tablename");
			connection = ConnectionFactory.createConnection (conf);

			if (!HBaseUtil.isExistTable (conf, tableName)) {
				HBaseUtil.initNamespace (conf, namespace);
				HBaseUtil.createTable (conf, tableName, regionsNum, "f1","f2");
			}
		} catch (IOException e) {
			e.printStackTrace ();
		}
	}

	/**
	 * ori数据样式： 18576581848,17269452013,2017-08-14 13:38:31,1761
	 * rowkey样式：01_18576581848_20170814133831_17269452013_1_1761
	 * HBase表的列：call1  call2   build_time   build_time_ts   flag   duration
	 *
	 * @param ori
	 */
	public void put(String ori) {
		try {
			/**优化处*/
			if(cacheList.size() == 0){
				connection = ConnectionInstance.getConnection(conf);
				table = (HTable) connection.getTable(TableName.valueOf(tableName));
				table.setAutoFlushTo(false);
				table.setWriteBufferSize(2 * 1024 * 1024);
			}

			String[] splitOri = ori.split (",");
			String caller = splitOri[0];
			String callee = splitOri[1];
			String buildTime = splitOri[2];
			String duration = splitOri[3];
			String regionCode = HBaseUtil.genRegionCode (caller, buildTime, regionsNum);

			String buildTimeReplace = sdf2.format (sdf1.parse (buildTime));
			String buildTimeTs = String.valueOf (sdf1.parse (buildTime).getTime ());

			//生成rowkey
			String rowkey = HBaseUtil.getRowKey (
					regionCode,
					caller,
					buildTimeReplace,
					callee,
					"1",
					duration);

			Put put = new Put (Bytes.toBytes (rowkey));
			put.addColumn (Bytes.toBytes ("f1"), Bytes.toBytes ("call1"), Bytes.toBytes (caller));
			put.addColumn (Bytes.toBytes ("f1"), Bytes.toBytes ("call2"), Bytes.toBytes (callee));
			put.addColumn (Bytes.toBytes ("f1"), Bytes.toBytes ("build_time"), Bytes.toBytes (buildTime));
			put.addColumn (Bytes.toBytes ("f1"), Bytes.toBytes ("build_time_ts"), Bytes.toBytes (buildTimeTs));
			put.addColumn (Bytes.toBytes ("f1"), Bytes.toBytes ("flag"), Bytes.toBytes ("1"));
			put.addColumn (Bytes.toBytes ("f1"), Bytes.toBytes ("duration"), Bytes.toBytes (duration));

			cacheList.add(put);

			/**优化处*/
			if(cacheList.size() >= 30){
				table.put(cacheList);
				table.flushCommits();

				table.close();
				cacheList.clear();
			}

		} catch (Exception e) {
			e.printStackTrace ();
		}
	}
	/*
 05_19043930474_20171118122551_17378239621_1_0888     column=f1:build_time, timestamp=1533904655204, value=2017-11-18 12:25:51
 05_19043930474_20171118122551_17378239621_1_0888     column=f1:build_time_ts, timestamp=1533904655204, value=1510979151000
 05_19043930474_20171118122551_17378239621_1_0888     column=f1:call1, timestamp=1533904655204, value=19043930474
 05_19043930474_20171118122551_17378239621_1_0888     column=f1:call2, timestamp=1533904655204, value=17378239621
 05_19043930474_20171118122551_17378239621_1_0888     column=f1:duration, timestamp=1533904655204, value=0888
 05_19043930474_20171118122551_17378239621_1_0888     column=f1:flag, timestamp=1533904655204, value=1
	* */
}
