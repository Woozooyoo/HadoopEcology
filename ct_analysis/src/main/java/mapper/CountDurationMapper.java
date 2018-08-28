package mapper;

import kv.key.ComDimension;
import kv.key.ContactDimension;
import kv.key.DateDimension;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CountDurationMapper extends TableMapper<ComDimension, Text> {
	private ComDimension comDimension = new ComDimension ();
	private Text durationText = new Text();
	private Map<String, String> phoneNameMap;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup (context);
		phoneNameMap = new HashMap<> ();
		phoneNameMap.put("17378239621", "陈泉");
		phoneNameMap.put("13522922704", "刘高铭");
		phoneNameMap.put("15566676447", "闵照奎");
		phoneNameMap.put("18381563239", "夏来旺");
		phoneNameMap.put("15766918297", "李少伟");
		phoneNameMap.put("18780554764", "危燕兵");
		phoneNameMap.put("15432684794", "林生华");
		phoneNameMap.put("17643135839", "孙宇杰");
		phoneNameMap.put("13550457973", "陈富");
		phoneNameMap.put("13131527573", "刘兆锋");
		phoneNameMap.put("18957722097", "张兰翔");
		phoneNameMap.put("14343778386", "岳庆");
		phoneNameMap.put("15407273951", "谢应强");
		phoneNameMap.put("15920873697", "卢宇鹏");
		phoneNameMap.put("15017051026", "黄蓓");
		phoneNameMap.put("18900049984", "熊宇辉");
		phoneNameMap.put("13944037807", "唐建东");
		phoneNameMap.put("15062350933", "易琴");
		phoneNameMap.put("19043930474", "马文明");
		phoneNameMap.put("15316339394", "肖东军");
		phoneNameMap.put("15848679847", "曹彬");
		phoneNameMap.put("17767135137", "吴中月");
		phoneNameMap.put("17500249899", "林一煌");
		phoneNameMap.put("18681920653", "刘智伟");
		phoneNameMap.put("18786175558", "张小平");
	}

	/**
	 * @Input param ImmutableBytesWritable 理解为RowKey
	 * @Input param Result：每一个该类型的实例化对象，都对应了一个rowkey中的若干数据
	 */
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		//05_19902496992_20170312154840_15542823911_1_1288
		String rowKey = Bytes.toString (key.get ());
		String[] splits = rowKey.split ("_");
		if (splits[4].equals ("0")) return;

		//以下数据全部是flag 为 1的主叫数据， 但也包含了被叫电话的数据
		String caller = splits[1];
		String callee = splits[3];
		String buildTime = splits[2];
		String duration = splits[5];
		durationText.set(duration);

		String year = buildTime.substring (0, 4);
		String month = buildTime.substring (4, 6);
		String day = buildTime.substring (6, 8);

		//组装ComDimension
		//组装DateDimension
		DateDimension yearDimension = new DateDimension (year, "-1", "-1");
		DateDimension monthDimension = new DateDimension (year, month, "-1");
		DateDimension dayDimension = new DateDimension (year, month, day);

		//组装ContactDimension
		ContactDimension callerContactDimension = new ContactDimension (caller, phoneNameMap.get (caller));

		//开始聚合主叫数据
		comDimension.setContactDimension (callerContactDimension);
		//年
		comDimension.setDateDimension (yearDimension);
		context.write (comDimension, durationText);
		//月
		comDimension.setDateDimension (monthDimension);
		context.write (comDimension, durationText);
		//日
		comDimension.setDateDimension (dayDimension);
		context.write (comDimension, durationText);

		//开始聚合被叫数据
		ContactDimension calleeContactDimension = new ContactDimension (callee, phoneNameMap.get (callee));
		comDimension.setContactDimension (calleeContactDimension);
		//年
		comDimension.setDateDimension (yearDimension);
		context.write (comDimension, durationText);
		//月
		comDimension.setDateDimension (monthDimension);
		context.write (comDimension, durationText);
		//日
		comDimension.setDateDimension (dayDimension);
		context.write (comDimension, durationText);
	}
}
