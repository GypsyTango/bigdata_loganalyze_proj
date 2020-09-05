package proj.logAnalyze.cdh.logClean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Author: Chen Lou
 * Version: 07/29/2020
 * 解析日志:访问者IP,访问时间,访问url,访问状态码,本次访问的流量
 */

public class logParser {
    // 当前格式
    public static final SimpleDateFormat CURRENT_TIME_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    // 期望格式
    public static final SimpleDateFormat EXPECTED_TIME_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss", Locale.ENGLISH);

    // 解析访问者ip
    public String ipParser(String line){
        String[] ips = line.split("- -");
        String ip = ips[0].trim();
        return ip;
    }
    // 解析访问时间
    public String timeParser(String line){
        int startIndex = line.indexOf("[");
        int endIndex = line.indexOf("+0800]");
        String time = line.substring(startIndex+1,endIndex);

        Date parse = null;
        try {
            parse = CURRENT_TIME_FORMAT.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return EXPECTED_TIME_FORMAT.format(parse);
    }
    // 解析访问url
    public String urlParser(String line){
        int indexStart = line.indexOf("\"");
        int indexEnd = line.lastIndexOf("\"");
        return line.substring(indexStart+1,indexEnd);
    }
    // 解析访问状态码
    public String statusParser(String line){
        String field = line.substring(line.lastIndexOf("\"") + 1).trim();
        return field.split(" ")[0];
    }
    // 解析本次访问的流量
    public String flowParser(String line){
        String field = line.substring(line.lastIndexOf("\"") + 1).trim();
        return field.split(" ")[1];
    }
    //组合以上解析过的字段 -> 数组中
    public String[] combineParser(String line){
        // 访问者IP,访问时间,访问url,访问状态码,本次访问的流量
        String ip = ipParser(line);
        String time = timeParser(line);
        String url = urlParser(line);
        String status = statusParser(line);
        String flow = flowParser(line);

        return new String[]{ip,time,url,status,flow};
    }

//    public static void main(String[] args) {
//        logParser logParser = new logParser();
//        String[] combineParser = logParser.combineParser("122.70.232.227 - - [31/May/2018:00:00:00 +0800] \"POST /forum.php?mod=post&action=reply&fid=122&tid=52774&extra=page%3D1&replysubmit=yes&infloat=yes&handlekey=fastpost&inajax=1 HTTP/1.1\" 200 451");
//        for (String cbp:combineParser){
//            System.out.println(cbp);
//        }
//    }

}
