package org.co2y.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


/**
 * Created by co2y on 16/4/5.
 */

/**
 * where fun_range_code(field, '00000000','[19500101,21001231]','99999999','[000000000000,999999999999]') = "true"
 * field类型为string, 四个公式都是String, hive目前不支持某个公式为null, 以"null"表示空
 */
public class fun_range_code extends UDF {
    private Text result = new Text();

    public Text evaluate(String field, String ifNUll, String formu1, String formu2, String formu3, String formu4) {
        result.set("false");

        String[] formus = {formu1, formu2, formu3, formu4};
        if (field.length() == 0) {
            if (ifNUll.equals("Y")) {
                result.set("true");
            } else {
                result.set("false");
            }
            return result;
        } else {
            for (String formu : formus) {

                if (formu.equals("null")) {
                    continue;
                } else if (!formu.contains("[")) {
                    if (Integer.parseInt(field) == Integer.parseInt(formu)) {
                        result.set("true");
                        return result;
                    }
                } else {
                    String[] range = formu.split(",");
                    int min = Integer.parseInt(range[0].substring(1, range[0].length()));
                    int max = Integer.parseInt(range[1].substring(0, range[1].length() - 1));
                    if (Integer.parseInt(field) <= max && Integer.parseInt(field) >= min) {
                        result.set("true");
                        return result;
                    }
                }

            }
        }
        return result;
    }
}
