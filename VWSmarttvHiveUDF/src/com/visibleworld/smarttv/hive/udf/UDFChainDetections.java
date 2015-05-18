package com.visibleworld.smarttv.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.text.SimpleDateFormat;
import java.util.*;

@UDFType(deterministic = false)
@Description(name="chainDetections")
public class UDFChainDetections extends UDF {
	private HashMap<String, String> ret = new HashMap<String, String>();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public UDFChainDetections() {}
	
	public HashMap<String, String> evaluate(List<String> dateList) {
		
		ArrayList<String> al = (ArrayList<String>) dateList;
		Date prev = null;
		String uniqueChainID = null;
		
		for (String s: al) {
			try {
				Date d = sdf.parse(s);
				if (prev == null) {
					prev = d;
					uniqueChainID = UUID.randomUUID().toString();
					ret.put(s, uniqueChainID);
				}
				else {
					long sec = (d.getTime()-prev.getTime())/1000;
					if (Math.abs(sec) <= 60 ){
						ret.put(s, uniqueChainID);
					}
					else {
						prev = d;
						uniqueChainID = UUID.randomUUID().toString();
						ret.put(s, uniqueChainID);						
					}
				}
			} catch (Exception e) {
				System.err.println("Error converting string to date");
				e.printStackTrace();
			}
			
		}
		
		return ret;
		
	}

}
