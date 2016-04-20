package org.co2y.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by co2y on 16/4/5.
 */

/**
 * where dateformat(field, "yyyy-MM-dd") = "true"
 * 也可以是yyyyMMdd
 * field类型为string
 */
public class dateformat extends UDF {
    private Text result = new Text();

    public Text evaluate(String field, String formu) {
        result.set("false");
        SimpleDateFormat format = new SimpleDateFormat(formu);
        try {
            format.setLenient(false);
            format.parse(field);
            result.set("true");
        } catch (ParseException e) {
            result.set("false");
        }
        return result;
    }
}
