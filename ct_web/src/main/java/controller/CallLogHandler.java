package controller;

import bean.CallLog;
import bean.QueryInfo;
import com.mysql.jdbc.StringUtils;
import dao.CallLogDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.HashMap;
import java.util.List;

@Controller
public class CallLogHandler {
	@Autowired
	CallLogDAO callLogDAO;

	@RequestMapping("/queryCallLogList")
	public String queryCallLog(Model model, QueryInfo queryInfo) {
		List<CallLog> list = null;
		HashMap<String, String> hashMap = new HashMap<> ();

		if (StringUtils.isNullOrEmpty (queryInfo.getMonth ())) {
			queryInfo.setYear ("2017");
			queryInfo.setMonth ("-1");
			queryInfo.setDay ("-1");

			hashMap.put ("year", queryInfo.getYear ());
			hashMap.put ("month", queryInfo.getMonth ());
			hashMap.put ("day", queryInfo.getDay ());
			hashMap.put ("telephone", queryInfo.getTelephone ());
			hashMap.put ("name", queryInfo.getName ());
			list = callLogDAO.getCallLogList (hashMap);

		} else {    //月份不为空
			queryInfo.setYear ("2017");
			queryInfo.setDay ("-1");
			hashMap.put ("year", queryInfo.getYear ());
			hashMap.put ("month", queryInfo.getMonth ());
			hashMap.put ("day", queryInfo.getDay ());
			hashMap.put ("telephone", queryInfo.getTelephone ());
			hashMap.put ("name", queryInfo.getName ());
			list = callLogDAO.getCallLogMonthList (hashMap);
		}

		StringBuilder dateSB = new StringBuilder ();
		StringBuilder callSumSB = new StringBuilder ();
		StringBuilder callDurationSumSB = new StringBuilder ();

		for (int i = 0; i < list.size (); i++) {
			CallLog callLog = list.get (i);
			if (queryInfo.getMonth () == "-1") {
				dateSB.append (callLog.getMonth () + "月,");//1月, 2月, ....12月,
			} else {
				dateSB.append (callLog.getDay () + "日,");//1日, 2日, ....31日,
			}
			callSumSB.append (callLog.getCall_sum () + ",");
			callDurationSumSB.append (String.valueOf (Integer.parseInt (callLog.getCall_duration_sum ()) / 60) + ",");
		}

		dateSB.deleteCharAt (dateSB.length () - 1);
		callSumSB.deleteCharAt (callSumSB.length () - 1);
		callDurationSumSB.deleteCharAt (callDurationSumSB.length () - 1);

		//通过model返回数据
		model.addAttribute ("telephone", list.get (0).getTelephone ());
		model.addAttribute ("name", list.get (0).getName ());
		model.addAttribute ("date", dateSB.toString ());
		model.addAttribute ("count", callSumSB.toString ());
		model.addAttribute ("duration", callDurationSumSB.toString ());

		return "jsp/CallLogListEchart";
	}
}
