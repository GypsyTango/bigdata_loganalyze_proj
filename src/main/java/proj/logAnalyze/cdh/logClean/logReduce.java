package proj.logAnalyze.cdh.logClean;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

// logReduce类的存在会减少小文件的数量，从而增加数据存储的效率
public class logReduce extends Reducer<NullWritable, Text, NullWritable, Text> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text v : values){
            context.write(NullWritable.get(),v);
        }
    }
}
