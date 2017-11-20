import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import com.hux.TfIdfCalculator;
import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;

import java.io.*;
import java.util.*;


/**
 * Created by david on 2017/8/17.
 */
public class testModel {
    public static String[] strs = null;
    public static HashSet<String> stopwords = new HashSet<String>();
    public static HashMap<String, String> idfSet = new HashMap<String, String>();
    public static ArrayList<ArrayList<String>> data = new ArrayList<ArrayList<String>>();


    public static void readIdf(){
        File file = new File("output/dictionary.txt");
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String text = null;

            while ((text = reader.readLine()) != null) {
                String[] sp = text.split(" ", 2);
                idfSet.put(sp[0], sp[1]);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
            }
        }
    }

    public static void readStopWords(){
        File file = new File("input/test/stopwords.txt");
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String text = null;

            while ((text = reader.readLine()) != null) {
                stopwords.add(text);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
            }
        }
    }
    public static void generateTestData(){
        readIdf();
        readStopWords();
        File file = new File("input/test/pachong_download.txt");
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            List<String> list = new ArrayList<String>();
            String text = null;

            while ((text = reader.readLine()) != null) {
                list.add(text);
            }

            strs = list.toArray(new String[list.size()]);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
            }
        }

        // jieba split
        JiebaSegmenter segmenter = new JiebaSegmenter();

        for (String sentence : strs) {
            ArrayList<String> llis = new ArrayList<String>();
            List a = segmenter.process(sentence, JiebaSegmenter.SegMode.INDEX);
            for(int i = 0;i < a.size();i++){
                String s = ((SegToken)a.get(i)).word;
                if(s.length() > 1 && !(s.charAt(0) >= '0' && s.charAt(0) <='9') && !(s.charAt(1) >= 'a' && s.charAt(1) <='z') &&
                !(s.charAt(1) >= '0' && s.charAt(1) <='9') && !stopwords.contains(s)){
                    llis.add(s);
                }
            }
            data.add(llis);
        }
        ArrayList<String> strss = new ArrayList<String>();
        try {
            PrintWriter writer = new PrintWriter("output/Data2Predict.txt", "UTF-8");
            TfIdfCalculator calculator = new TfIdfCalculator();

            for (int i = 0; i < data.size(); i++) {
                String temp = "1";
                for (int j = 0; j < data.get(i).size(); j++) {
                    String wword = data.get(i).get(j);
                    if (idfSet.containsKey(wword)) {
                        String[] idfAndIndex = idfSet.get(wword).split(" ");
                        double tf = calculator.tf(data.get(i), wword);
                        double idf = Double.parseDouble(idfAndIndex[0]);
                        int index = Integer.parseInt((idfAndIndex[1]));
                        double tfidf = tf * idf;
                        temp += " " + Integer.toString(index) + ":" + Double.toString(tfidf);
                    }
                }
                strss.add(temp);
                writer.println(temp);
            }
            writer.close();
        }catch(IOException e){

        }
//        String[] predictArgs = {"output/Data2Predict.txt", "vector.txt.model", "output/predictOut.txt"};
//        try {
//            svm_predict.main(predictArgs);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        svm_model model = null;
        try {
            model = svm.svm_load_model("vector.txt.model");
        } catch (IOException e) {
            e.printStackTrace();
        }
        for(String str : strss){
            StringTokenizer st = new StringTokenizer(str," \t\n\r\f:");

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
            System.out.println(v);
        }

    }
    public static void main(String[] args){
        generateTestData();
    }
}
