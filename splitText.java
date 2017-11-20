import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import com.hux.TfIdfCalculator;
import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by david on 2017/8/21.
 */
public class splitText {
    public static class GenerateVectorMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text titlevalue = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            String title = "null", fileName = "null";
            if(strs.length >= 5) {
                title = strs[4];
                fileName = strs[0] + strs[1];
            }
            word.set(fileName);
            titlevalue.set(title);
            context.write(word, titlevalue);
        }
    }

    public static class ClassifyReducer
            extends Reducer<Text, Text, Text, NullWritable> {
        private static svm_model model = null;
        private static JiebaSegmenter segmenter = new JiebaSegmenter();
        private static String[] strs = null;
        private static HashSet<String> stopwords = new HashSet<String>();

        private void readFile(BufferedReader idfBr, BufferedReader stopWordsBr) throws IOException {
            String text = null;
            while ((text = stopWordsBr.readLine()) != null) {
                stopwords.add(text);
            }
        }


        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String modelpath = conf.get("modelpath");
//            Path pt=new Path(modelpath + "vector.txt.model");//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(modelpath + "vector.txt.model"))));
            BufferedReader idfbr=new BufferedReader(new InputStreamReader(fs.open(new Path(modelpath + "dictionary.txt"))));
            BufferedReader spbr=new BufferedReader(new InputStreamReader(fs.open(new Path(modelpath + "stopwords.txt"))));
            readFile(idfbr, spbr);
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Text valueOut = new Text();
            for(Text val : values){
                String valueStr = val.toString();
                ArrayList<String> llis = new ArrayList<String>();
                List a = segmenter.process(valueStr, JiebaSegmenter.SegMode.INDEX);
                for(int i = 0;i < a.size();i++){
                    String s = ((SegToken)a.get(i)).word;
                    if(s.length() > 1 && !(s.charAt(0) >= '0' && s.charAt(0) <='9') && !(s.charAt(1) >= 'a' && s.charAt(1) <='z') &&
                            !(s.charAt(1) >= '0' && s.charAt(1) <='9') && !stopwords.contains(s)){
                        llis.add(s);
                    }
                }
                String vectorStr = "";

                for (String s : llis) {
                    vectorStr += s + " ";
                }

                valueOut.set(vectorStr);
                context.write(valueOut, NullWritable.get());
            }
        }
    }

    private static void exit_with_help()
    {
        System.out.print(
                "Usage: hadoop jar Classify modelDir inputDir outputDir\n"
        );
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 3) {
            System.out.println(args.length);
            exit_with_help();
        }


        Configuration conf = new Configuration();
        conf.set("modelpath", args[0]);
        Job job = Job.getInstance(conf, "split text");
        job.setJarByClass(splitText.class);
        job.setMapperClass(splitText.GenerateVectorMapper.class);
//        job.setCombinerClass(ClassifyText.IntSumReducer.class);
        job.setReducerClass(splitText.ClassifyReducer.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
