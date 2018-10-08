package com.blueasdfg.step2;

import com.blueasdfg.step1.MR1;
import com.blueasdfg.step1.Mapper1;
import com.blueasdfg.step1.Reducer1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MR2 {

     private static String inPath = "hdfs://bgybdos/bd-os/jupiter/home/gh/step2_input/matrix1.txt";

     private  static String outPath = "hdfs://bgybdos/bd-os/jupiter/home/gh/step2_output";

     private static String cache = "hdfs://bgybdos/bd-os/jupiter/home/gh/step1_output/part-r-00000";

     private static  String hdfs = "hdfs://bgybdos";

     public int run() throws IOException {

          Configuration conf = new Configuration();
          conf.set("fs.defaultFS", hdfs);

          try {
               Job job = Job.getInstance(conf, "step2");

               job.addCacheArchive(new URI(cache+ "#matrix2"));

               job.setJarByClass(MR2.class);
               job.setMapperClass(Mapper2.class);
               job.setReducerClass(Reducer2.class);

               job.setMapOutputKeyClass(Text.class);
               job.setMapOutputValueClass(Text.class);

               job.setOutputKeyClass(Text.class);
               job.setOutputValueClass(Text.class);

               FileSystem fs = FileSystem.get(conf);

               Path inputPath = new Path(inPath);
               FileInputFormat.addInputPath(job, inputPath);

               Path outputPath = new Path(outPath);
               FileOutputFormat.setOutputPath(job, outputPath);

               return job.waitForCompletion(true)?1:-1;

          }catch (IOException e){
               e.printStackTrace();
          } catch (InterruptedException e) {
               e.printStackTrace();
          } catch (ClassNotFoundException e) {
               e.printStackTrace();
          } catch (URISyntaxException e) {
               e.printStackTrace();
          }
          return -1;


     }


     public static void main(String[] arg) throws Exception {
          int result = -1;
          result = new MR2().run();
     }


}
