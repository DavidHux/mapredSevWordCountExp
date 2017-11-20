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
public class ClassifyText {
    public static class GenerateVectorMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text titlevalue = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
//            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
//            System.out.println(fileName + value.toString());
//            StringTokenizer itr = new StringTokenizer(value.toString());
//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken());
//                context.write(word, one);
//            }
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
            extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();
        private static svm_model model = null;
        private static JiebaSegmenter segmenter = new JiebaSegmenter();
        private static String[] strs = null;
        private static HashSet<String> stopwords = new HashSet<String>();
        private static HashMap<String, String> idfSet = new HashMap<String, String>();

        private void readFile(BufferedReader idfBr, BufferedReader stopWordsBr) throws IOException {
            String text = null;
            while ((text = idfBr.readLine()) != null) {
                String[] sp = text.split(" ", 2);
                idfSet.put(sp[0], sp[1]);
            }
            while ((text = stopWordsBr.readLine()) != null) {
                stopwords.add(text);
            }
        }
        private String generateVec(String title){

            ArrayList<String> llis = new ArrayList<String>();
            List a = segmenter.process(title, JiebaSegmenter.SegMode.INDEX);
            for(int i = 0;i < a.size();i++){
                String s = ((SegToken)a.get(i)).word;
                if(s.length() > 1 && !(s.charAt(0) >= '0' && s.charAt(0) <='9') && !(s.charAt(1) >= 'a' && s.charAt(1) <='z') &&
                        !(s.charAt(1) >= '0' && s.charAt(1) <='9') && !stopwords.contains(s)){
                    llis.add(s);
                }
            }

            TfIdfCalculator calculator = new TfIdfCalculator();

            String temp = "1";
            for (int j = 0; j < llis.size(); j++) {
                String wword = llis.get(j);
                if (idfSet.containsKey(wword)) {
                    String[] idfAndIndex = idfSet.get(wword).split(" ");
                    double tf = calculator.tf(llis, wword);
                    double idf = Double.parseDouble(idfAndIndex[0]);
                    int index = Integer.parseInt((idfAndIndex[1]));
                    double tfidf = tf * idf;
                    temp += " " + Integer.toString(index) + ":" + Double.toString(tfidf);
                }
            }
            return temp;
        }


        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String modelpath = conf.get("modelpath");
//            Path pt=new Path(modelpath + "vector.txt.model");//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
//            System.out.println(modelpath);
//            modelpath = "model/";
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(modelpath + "vector.txt.model"))));
            BufferedReader idfbr=new BufferedReader(new InputStreamReader(fs.open(new Path(modelpath + "dictionary.txt"))));
            BufferedReader spbr=new BufferedReader(new InputStreamReader(fs.open(new Path(modelpath + "stopwords.txt"))));
            model = svm.svm_load_model(br);
            readFile(idfbr, spbr);
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//            result.set(sum);
            Text valueOut = new Text();
            for(Text val : values){
                String valueStr = val.toString();
                System.out.println("123" + valueStr);
                String vectorStr = generateVec(valueStr);
                StringTokenizer st = new StringTokenizer(vectorStr," \t\n\r\f:");

                double target = Double.parseDouble(st.nextToken());
                int m = st.countTokens()/2;
                svm_node[] x = new svm_node[m];
                for(int j=0;j<m;j++)
                {
                    x[j] = new svm_node();
                    x[j].index = Integer.parseInt(st.nextToken());
                    x[j].value = Double.parseDouble(st.nextToken());
                }
                double v = svm.svm_predict(model,x);
                valueOut.set(Double.toString(v) + " " + valueStr);
                context.write(key, valueOut);
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
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(ClassifyText.class);
        job.setMapperClass(ClassifyText.GenerateVectorMapper.class);
//        job.setCombinerClass(ClassifyText.IntSumReducer.class);
        job.setReducerClass(ClassifyText.ClassifyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
