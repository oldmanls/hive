package org.co2y.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by co2y on 16/4/5.
 */


/**
 * where numlength(field, 14, 2, "N") = "true"
 * field类型为double
 */

public class numlength extends UDF {
    private Text result = new Text();

    public Text evaluate(Double field, int ilength, int dlength, String yesOrNo) {
        String sfield = field + "";
        result.set("false");
        if (sfield.length() == 0) {
            if (yesOrNo.equals("Y")) {
                result.set("true");
            }

            return result;

        }
        String[] values = sfield.split("\\.");
        if (values[0].length() <= ilength) {
            if (values.length == 1) {
                result.set("true");
            } else {
                if (values[1].length() <= dlength) {
                    result.set("true");
                } else {
                    result.set("false");
                }
            }
        } else {
            result.set("false");
        }
        return result;
    }
}
