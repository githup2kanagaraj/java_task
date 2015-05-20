package com.smarttv.hive.viewerclassification;

/**
 * Created by kanagaraj on 18/5/15.
 */

import com.google.common.base.Splitter;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.util.HashMap;
import java.util.Map;


@UDFType(deterministic = false)
@Description(name="selectcohortvalue")

public class selectcohortvalue extends UDF {

    public selectcohortvalue() {}

    public String evaluate(String cohort_string, String select_key)
    {
       if (cohort_string!=null)
       {
            Map<String, String> cohortmap = new HashMap<String, String>();
            cohortmap = Splitter.on(",").withKeyValueSeparator("~").split(cohort_string);
            String value = cohortmap.get(select_key);
            return value;
        }
       else
       {
            return null;
       }
    }

}
