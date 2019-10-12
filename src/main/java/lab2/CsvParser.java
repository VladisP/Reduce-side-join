package lab2;

import org.apache.hadoop.io.Text;

public class CsvParser {

    public static String[] getColumns(Text row, boolean needHack) {

        if (needHack) {
            return row.toString().replaceAll(",", ",\"").split(",");
        } else {
            return row.toString().replaceAll("\"", "").split(",");
        }
    }

    public static String getColumn(String[] columns, int index) {

        return columns[index].replaceAll("\"", "");
    }
}
