package vwsmarttv.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.*;

@UDFType(deterministic = false)
@Description(name="cohortselection")

public class cohortselection extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        ObjectInspector a = args[0];
        ObjectInspector b = args[1];

        this.listOI = (MaObjectInspector) a;
        return null;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        HashMap<String,Double> hivemap =  this.h
        return null;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "cohortselection()"; // this should probably be better
    }

    public String evaluate(String k ,HashMap<String,Double> hivemap)
    {

        return hivemap.get(k).toString();
    }
}
