import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

import java.io.IOException;
import java.util.List;

public class combineFile extends Configured implements Tool {
        private static final Log LOG = LogFactory.getLog(combineFile.class);
        private static final long ONE_MB = 1024 * 1024L;

        static class TextFileMapper extends Mapper<LongWritable , Text, Text, Text> {

//            @Override
            protected void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {
//                Configuration configuration = context.getConfiguration();
                LOG.warn("#######################" + ((FileSplit) context.getInputSplit()).getPath());
                Text filenameKey = new Text("" + ((FileSplit) context.getInputSplit()).getPath());
                context.write(filenameKey, value);
            }
        }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            int exitCode = ToolRunner.run(conf, new combineFile(), args);
            System.exit(exitCode);
        }

//        @Override
        public int run(String[] args) throws Exception {
            Configuration conf = new Configuration(getConf());
            conf.set("mapreduce.input.fileinputformat.split.maxsize", ""+ ONE_MB * 32);
            Job job = Job.getInstance(conf);
            FileInputFormat.setInputPaths(job, args[0]);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setJarByClass(combineFile.class);
            job.setInputFormatClass(CombineFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(TextFileMapper.class);
            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

