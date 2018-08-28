package producer;

import java.io.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ProductLog {
	private String startTime = "2017-01-01";
	private String endTime = "2017-12-31";

	//生产数据
	//用于存放待随机的电话号码
	private List<String> phoneList = new ArrayList<> ();
	private Map<String, String> phoneNameMap = new HashMap<> ();

	public void initPhone() {
		phoneList.add ("17378239621");
		phoneList.add ("13522922704");
		phoneList.add ("15566676447");
		phoneList.add ("18381563239");
		phoneList.add ("15766918297");
		phoneList.add ("18780554764");
		phoneList.add ("15432684794");
		phoneList.add ("17643135839");
		phoneList.add ("13550457973");
		phoneList.add ("13131527573");
		phoneList.add ("18957722097");
		phoneList.add ("14343778386");
		phoneList.add ("15407273951");
		phoneList.add ("15920873697");
		phoneList.add ("15017051026");
		phoneList.add ("18900049984");
		phoneList.add ("13944037807");
		phoneList.add ("15062350933");
		phoneList.add ("19043930474");
		phoneList.add ("15316339394");
		phoneList.add ("15848679847");
		phoneList.add ("17767135137");
		phoneList.add ("17500249899");
		phoneList.add ("18681920653");
		phoneList.add ("18786175558");

		phoneNameMap.put ("17378239621", "陈泉");
		phoneNameMap.put ("13522922704", "刘高铭");
		phoneNameMap.put ("15566676447", "闵照奎");
		phoneNameMap.put ("18381563239", "夏来旺");
		phoneNameMap.put ("15766918297", "李少伟");
		phoneNameMap.put ("18780554764", "危燕兵");
		phoneNameMap.put ("15432684794", "林生华");
		phoneNameMap.put ("17643135839", "孙宇杰");
		phoneNameMap.put ("13550457973", "陈富");
		phoneNameMap.put ("13131527573", "刘兆锋");
		phoneNameMap.put ("18957722097", "张兰翔");
		phoneNameMap.put ("14343778386", "岳庆");
		phoneNameMap.put ("15407273951", "谢应强");
		phoneNameMap.put ("15920873697", "卢宇鹏");
		phoneNameMap.put ("15017051026", "黄蓓");
		phoneNameMap.put ("18900049984", "熊宇辉");
		phoneNameMap.put ("13944037807", "唐建东");
		phoneNameMap.put ("15062350933", "易琴");
		phoneNameMap.put ("19043930474", "马文明");
		phoneNameMap.put ("15316339394", "肖东军");
		phoneNameMap.put ("15848679847", "曹彬");
		phoneNameMap.put ("17767135137", "吴中月");
		phoneNameMap.put ("17500249899", "林一煌");
		phoneNameMap.put ("18681920653", "刘智伟");
		phoneNameMap.put ("18786175558", "张小平");
	}

	/**
	 * 形式：15837312345,13737312345,2017-01-09 08:09:10,0360
	 */
	public String product() {
		String caller = null;
		String callee = null;

		String callerName = null;
		String calleeName = null;

		//取得主叫电话号码
		int callerIndex = (int) (Math.random () * phoneList.size ());
		caller = phoneList.get (callerIndex);
		callerName = phoneNameMap.get (caller);

		//取得被叫电话号码
		while (true) {
			int calleeIndex = (int) (Math.random () * phoneList.size ());
			callee = phoneList.get (calleeIndex);
			calleeName = phoneNameMap.get (callee);
			if (!caller.equals (callee)) break;
		}

		String buildTime = randomBuildTime (startTime, endTime);
		//0000 用DecimalFormat
		DecimalFormat df = new DecimalFormat ("0000");
		String duration = df.format ((int) (30 * 60 * Math.random ()));
		StringBuilder sb = new StringBuilder ();
		sb.append (caller + ",").append (callee + ",").append (buildTime + ",").append (duration);
		return sb.toString ();
	}

	/**
	 * 根据传入的时间区间，在此范围内随机通话建立的时间
	 * startTimeTS + (endTimeTs - startTimeTs) * Math.random();
	 *
	 * @param startTime
	 * @param endTime
	 */
	public String randomBuildTime(String startTime, String endTime) {
		try {
			SimpleDateFormat sdf1 = new SimpleDateFormat ("yyyy-MM-dd");
			Date startDate = sdf1.parse (startTime);
			Date endDate = sdf1.parse (endTime);

			if (endDate.getTime () <= startDate.getTime ()) return null;

			long randomTS = startDate.getTime () + (long) ((endDate.getTime () - startDate.getTime ()) * Math.random ());
			Date resultDate = new Date (randomTS);
			SimpleDateFormat sdf2 = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");
			String resultTimeString = sdf2.format (resultDate);
			return resultTimeString;
		} catch (ParseException e) {
			e.printStackTrace ();
		}
		return null;
	}

	/**
	 * 将数据写入到文件中
	 */
	public void writeLog(String filePath) {
		try {
			OutputStreamWriter osw = new OutputStreamWriter (new FileOutputStream (filePath, true), "UTF-8");
			while (true) {
				Thread.sleep (500);
				String log = product ();
				System.out.println (log);
				osw.write (log + "\n");
				//一定要手动flush才可以确保每条数据都写入到文件一次
				osw.flush ();
			}
		} catch (Exception e) {
			e.printStackTrace ();
		}
	}

	public static void main(String[] args) throws InterruptedException {
		if (args == null || args.length <= 0) {
			System.out.println ("no arguments");
			return;
		}
//        String logPath = "D:\\calllog.csv";
		ProductLog productLog = new ProductLog ();
		productLog.initPhone ();
		productLog.writeLog (args[0]);
	}
}
