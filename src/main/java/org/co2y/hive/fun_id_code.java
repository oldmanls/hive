package org.co2y.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by co2y on 16/4/5.
 */
public class fun_id_code extends UDF {
    private Text result = new Text();

    public Text evaluate(String field) {
        String formu = "(^[0-9]{17}[0-9|x]$)|(^[0-9]{15}$)";
        Pattern p = Pattern.compile(formu);
        Matcher m = p.matcher(field);

        if (m.find()) {
            result.set("true");
        } else {
            result.set("false");
        }
        return result;
    }
}
