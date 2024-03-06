import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Exam1 {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Users\\PZ_EDU\\Desktop\\资料\\hadoop-2.9.2");
        Configuration conf = new Configuration();
        //获取集群访问路径
        FileSystem fileSystem = FileSystem.get(conf);
        //判断输出路径是否已经存在，如果存在则删除
        Path outPath = new Path("C:\\Users\\PZ_EDU\\Desktop\\资料\\out");
        if (fileSystem.exists(outPath))
            fileSystem.delete(outPath, true);
        //生成job，并指定job的名称
        Job job = Job.getInstance(conf, "Exam1");
        //指定打成jar后的运行类
        job.setJarByClass(Exam1.class);
        //指定mapper类
        job.setMapperClass(MyMapper.class);
        //指定mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //指定reducer类 
        job.setReducerClass(MyReducer.class);
        //指定reduce的输出类型 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //数据的输入目录
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\PZ_EDU\\Desktop\\资料\\数据集"));
        //数据的输出目录
        FileOutputFormat.setOutputPath(job, outPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // 统计数量
//    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
//        @Override
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String[] line = value.toString().split(",");
//            context.write(new Text(line[0]), new LongWritable(1));
//        }
//    }
//
//
//    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
//        @Override
//        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//            long sum = 0;
//            for (LongWritable value: values)
//                sum += value.get();
//            context.write(key,new LongWritable(sum));
//        }
//    }
//    求平均数
    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                String[] line = value.toString().split(",");
                context.write(new Text(line[0]), new LongWritable(Long.parseLong(line[8])));
            } catch (Exception e) {
                return;
            }
        }

    }

    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0, count = 0;
            for (LongWritable value : values) {
                sum += value.get();
                count++;
            }
            context.write(key, new LongWritable(count));
            context.write(key, new LongWritable(sum / count));
        }
    }
//    求最大值(最小值同理）
//    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
//        @Override
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//
//            try {
//                String[] line = value.toString().split(",");
//                context.write(new Text(line[0]), new LongWritable(Long.parseLong(line[8])));
//            } catch (Exception e) {
//                return;
//            }
//        }
//
//    }
//
//    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
//        @Override
//        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//            long sum = 0, max = 0;
//            for (LongWritable value : values) {
//                if (max < value.get()) {
//                    max = value.get();
//                }
//            }
//            context.write(key, new LongWritable(max));
//        }
//    }
}