import com.hux.TfIdfCalculator;

import java.io.*;
import java.util.*;

/**
 * Created by david on 2017/8/17.
 */
public class generateTrainModel {
    public static String path = "input/traindata/";
    public static String[] trainfilenames = new String[]{"negative", "neutral", "positive"};
    public static String chiWordsFilePath = "/Users/David/Desktop/data/chi_words.txt";

    public HashMap<String, Double> stringIdf = new HashMap<String, Double>();
    public List<List<String>>[] docsClasses = new ArrayList[3];
    public List<List<String>> documents = new ArrayList<List<String>>(); //Arrays.asList(
    public List<String> vectors = new ArrayList<String>();
    public List<String> dictionary = new ArrayList<String>();
    public HashSet<String> chiWords = new HashSet<String>();

    public void readChiWords(){
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(chiWordsFilePath));
            String text = null;

            while ((text = reader.readLine()) != null) {
                chiWords.add(text);
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

    public void readDocs(){
        for(int i = 0; i < 3; i++){
            List<List<String>> docsClass1 = new ArrayList<List<String>>();
            File file = new File(path+ trainfilenames[i] +".txt");
            BufferedReader reader = null;

            try {
                reader = new BufferedReader(new FileReader(file));
                String text = null;

                while ((text = reader.readLine()) != null) {
                    List<String> list = new ArrayList<String>();
                    list.addAll(Arrays.asList(text.split(" ")));
                    Iterator<String> it = list.iterator();
                    while(it.hasNext()){
                        if(!chiWords.contains(it.next()))
                            it.remove();
                    }
                    documents.add(list);
                    docsClass1.add(list);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                docsClasses[i] = docsClass1;
                try {
                    if (reader != null) {
                        reader.close();
                    }
                } catch (IOException e) {
                }
                for(List<String> ll : documents){
                    for(String str : ll)
                        System.out.print(str);
                    System.out.println();
                }
            }
        }
    }
    public void generateDictionary(){
        for(int i = 0;i < documents.size();i++){
            for(int j = 0;j < documents.get(i).size();j++){
                if(dictionary.indexOf(documents.get(i).get(j)) == -1){
                    dictionary.add(documents.get(i).get(j));
                }
            }
        }
    }
    public void generateVector() {
        try {
            PrintWriter writer = new PrintWriter("output/vector.txt", "UTF-8");
            TfIdfCalculator calculator = new TfIdfCalculator();
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < docsClasses[i].size(); j++) {
                    String sss = Integer.toString(i +1);
                    List<String> sent = docsClasses[i].get(j);
                    for (int k = 0; k < sent.size(); k++) {
                        if (!isChecked(sent, sent.get(k), k)) {
                            double tf = calculator.tf(sent, sent.get(k));
                            double idf = stringIdf.get(sent.get(k));
                            double tfidf = tf * idf;
                            int indexOfStr = dictionary.indexOf(sent.get(k));
                            sss += " " + Integer.toString(indexOfStr) + ":" + Double.toString(tfidf);
                        }
                    }
                    vectors.add(sss);
                    writer.println(sss);
                }
            }
            writer.close();
        } catch (IOException e){

        }
    }
    public void generateIdf(){
        TfIdfCalculator calculator = new TfIdfCalculator();
        for(int i = 0; i < dictionary.size();i++){
            double idf = calculator.idf(documents, dictionary.get(i));
            stringIdf.put(dictionary.get(i), idf);
        }
    }
    public boolean isChecked(List<String> strs, String str, int index){
        for(int i = 0; i < index; i++){
            if(strs.get(i) == str)
                return true;
        }
        return false;
    }

    public void generateModel(){
        try {
            svm_train.main(new String[]{"-t", "0", "output/vector.txt"});
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void writeidf(){
        try {
            PrintWriter writer = new PrintWriter("output/dictionary.txt", "UTF-8");
            TfIdfCalculator calculator = new TfIdfCalculator();
            Iterator it = stringIdf.entrySet().iterator();
            while(it.hasNext()){
                HashMap.Entry<String, Double> e = (HashMap.Entry<String, Double>)it.next();
                String ss = "";
                ss += e.getKey() + " " + Double.toString(e.getValue()) + " " + Integer.toString(dictionary.indexOf(e.getKey()));
                writer.println(ss);

            }
            writer.close();
        } catch (IOException e){

        }
    }





    public static void main(String[] args){
        generateTrainModel ge = new generateTrainModel();
//        ge.readChiWords();
//        ge.readDocs();
//        ge.generateDictionary();
//        ge.generateIdf();
//        ge.generateVector();
//        ge.writeidf();
        ge.generateModel();
    }
}
