/**
 * Created by co2y on 16/3/24.
 */
package org.co2y.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


/**
 * where charcontain(field, "社会公众股|国家法人股", "Y") = "true"
 * field类型为string
 */
public class charcontain extends UDF {
    private Text result = new Text();

    public Text evaluate(String field, String formu, String yesOrNo) {

        result.set("false");
        if (field.length() == 0) {
            if (yesOrNo.equals("Y")) {
                result.set("true");
            }
            return result;
        }
        String[] values = formu.split("\\|");
        for (String value : values) {
            if (field.contains(value)) {
                result.set("true");
            }
        }
        return result;
    }
}
