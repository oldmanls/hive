package org.co2y.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by co2y on 16/4/1.
 */

/**
 * where charlength(field, "8-160", "Y") = "true"
 * field类型为string
 */
public class charlength extends UDF {
    private Text result = new Text();

    public Text evaluate(String field, String formu, String yesOrNo) {
        result.set("false");
        if (field.length() == 0) {
            if (yesOrNo.equals("Y")) {
                result.set("true");
            }
            return result;
        }
        if (formu.contains("-")) {
            int min = Integer.parseInt(formu.split("-")[0]);
            int max = Integer.parseInt(formu.split("-")[1]);
            if (field.length() >= min && field.length() <= max) {
                result.set("true");
            }
        }
        return result;
    }
}
