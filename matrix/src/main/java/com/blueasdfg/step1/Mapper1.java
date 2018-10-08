package com.blueasdfg.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

     private Text outkey = new Text();
     private Text outValue = new Text();

     @Override
     protected void map(LongWritable key, Text value, Context context)
             throws IOException, InterruptedException {
          String[] rowAndLine = value.toString().split("\t");

          String row = rowAndLine[0];
          String[] lines = rowAndLine[1].split(",");

          for (int i=0; i < lines.length; i++){
               String colnum = lines[i].split("_")[0];
               String valueStr = lines[i].split("_")[1];

               outkey.set(colnum);
               outValue.set(row + "_" + valueStr);
               context.write(outkey, outValue);
          }
     }
}



















