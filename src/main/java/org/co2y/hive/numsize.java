package org.co2y.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by co2y on 16/4/5.
 * where (field,">=90","Y") = true
 * field为double型
 */
public class numsize extends UDF {
    private Text result = new Text();

    public Text evaluate(Double field, String formu, String yesOrNo) {
        result.set("false");
        if ((field + "").length() == 0) {
            if (yesOrNo.equals("Y")) {
                result.set("true");
            }
            return result;
        }
// 0:=  1:>  2:>  3:>=  4:<=  5:<>


        if (formu.startsWith("<>")) {
            if (field != Integer.parseInt(formu.substring(2))) {
                result.set("true");
            }
        } else if (formu.startsWith("<=")) {
            if (field <= Integer.parseInt(formu.substring(2))) {
                result.set("true");
            }
        } else if (formu.startsWith(">=")) {
            if (field >= Integer.parseInt(formu.substring(2))) {
                result.set("true");
            }
        } else if (formu.startsWith("<")) {
            if (field < Integer.parseInt(formu.substring(1))) {
                result.set("true");
            }
        } else if (formu.startsWith(">")) {
            if (field > Integer.parseInt(formu.substring(1))) {
                result.set("true");
            }
        } else if (formu.startsWith("=")) {
            if (field == Integer.parseInt(formu.substring(1))) {
                result.set("true");
            }
        }

        return result;
    }

}
