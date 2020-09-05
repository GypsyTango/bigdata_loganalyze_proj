package proj.logAnalyze.cdh.logClean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class logMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    // 通过调用解析类'logParser'获取解析后的数据
    logParser logParser = new logParser();
    Text logOutValue = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 对日志进行清洗
        String line = value.toString();
        String[] parser = logParser.combineParser(line);
        // 过滤静态资源请求
        if(parser[2].startsWith("GET /static") || parser[2].startsWith("GET /uc_server")){
            return;
        }
        // 过滤'GET'和'POST'头部字符串
        if(parser[2].startsWith("GET /")){
            parser[2] = parser[2].substring(parser[2].indexOf("/")+5);
        } else if(parser[2].startsWith("POST /")){
            parser[2] = parser[2].substring(parser[2].indexOf("/")+1);
        } else if(parser[2].startsWith("GET ")){
            parser[2] = parser[2].substring(parser[2].indexOf("GET ")+"GET ".length());
        }
        // 过滤'HTTP/1.1'和'HTTP/1.0'尾部字符串
        if(parser[2].endsWith("HTTP/1.1")){
            parser[2] = parser[2].split(" ")[0];
        }else if(parser[2].endsWith("HTTP/1.0")){
            parser[2] = parser[2].split(" ")[0];
        }
        // 输出清洗后的数据
        logOutValue.set(parser[0]+"\t"+parser[1]+"\t"+parser[2]);
        context.write(NullWritable.get(), logOutValue);
    }
}
