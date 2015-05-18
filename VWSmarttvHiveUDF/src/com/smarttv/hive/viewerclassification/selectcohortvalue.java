package com.smarttv.hive.viewerclassification;

/**
 * Created by kanagaraj on 18/5/15.
 */

import com.google.common.base.Splitter;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.util.Map;

@UDFType(deterministic = false)
@Description(name="selectcohortvalue")

public class selectcohortvalue {

    private String map_function(String cohort_string, int select_key)
    {
        Map<String, String> cohortmap;

        {
            cohortmap = Splitter.on(",").withKeyValueSeparator(":").split(cohort_string);
        }

        String value;
            value = cohortmap.get(select_key);
        return  value;
    }

    public static  void main(String args[])
    {
        selectcohortvalue obj = new selectcohortvalue();

        System.out.println(obj.map_function("{100 :kanagaraj,200: raj}", 100));
    }

}
