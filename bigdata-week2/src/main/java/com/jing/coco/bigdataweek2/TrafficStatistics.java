package com.jing.coco.bigdataweek2;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrafficStatistics {


    public static class TrafficStatisticsMapper extends Mapper<Object, Text, Text, PhoneBeanDTO> {

        public void map(Object object, Text value, Context context) throws IOException, InterruptedException {
            String[] rawLogFields = value.toString().split("\t");
            String phoneNum = rawLogFields[1];
            String up = rawLogFields[8];
            String down = rawLogFields[8];
            PhoneBeanDTO phoneBeanDTO = new PhoneBeanDTO();
            phoneBeanDTO.setUp(Long.valueOf(up));
            phoneBeanDTO.setDown(Long.valueOf(down));
            phoneBeanDTO.setTotalSize(phoneBeanDTO.getUp() + phoneBeanDTO.getDown());
            context.write(new Text(phoneNum), phoneBeanDTO);
        }

    }


    public static class TrafficStatisticsReducer extends Reducer<Text, PhoneBeanDTO, Text, PhoneBeanDTO> {

        private PhoneBeanDTO phoneBeanDTO = new PhoneBeanDTO();

        public void reduce(Text key, Iterable<PhoneBeanDTO> phoneBeanDTOIterable, Context context)
            throws IOException, InterruptedException {
            long up = 0;
            long down = 0;

            for (PhoneBeanDTO phoneBeanDTO : phoneBeanDTOIterable) {
                up += phoneBeanDTO.getUp();
                down += phoneBeanDTO.getDown();
            }
            this.phoneBeanDTO.setUp(up);
            this.phoneBeanDTO.setDown(down);
            this.phoneBeanDTO.setTotalSize(up + down);
            context.write(key, this.phoneBeanDTO);
        }
    }


    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration, "traffic statistics");
            job.setJarByClass(TrafficStatistics.class);
            job.setMapperClass(TrafficStatisticsMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(PhoneBeanDTO.class);
            job.setReducerClass(TrafficStatisticsReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(PhoneBeanDTO.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
