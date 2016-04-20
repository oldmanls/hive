package org.co2y.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by co2y on 16/4/1.
 */

/**
 * where charformat(field) = "true"
 * 只包含数字和大写字母
 * field类型为string
 */
public class charformat extends UDF {
    private Text result = new Text();

    public Text evaluate(String field) {
        Pattern p = Pattern.compile("[^0-9A-Z]");
        Matcher m = p.matcher(field);

        if (m.find()) {
            result.set("false");
        } else {
            result.set("true");
        }
        return result;
    }

}
