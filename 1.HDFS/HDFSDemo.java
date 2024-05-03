import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
	
	public class Main {
		public static void createDirectory() throws IOException {
	        Configuration configuration = new Configuration();
	        configuration.set("fs.defaultFS", "hdfs://10.0.2.15");
	        FileSystem fileSystem = FileSystem.get(configuration);
	        String directoryName = "BigdataHUMG/HDFSexample";
	        Path path = new Path(directoryName);
	        fileSystem.mkdirs(path);
	    }
		
		public static void writeFileToHDFS() throws IOException {
	        Configuration configuration = new Configuration();
	        configuration.set("fs.defaultFS", "hdfs://10.0.2.15");
	        FileSystem fileSystem = FileSystem.get(configuration);
	        //Create a path
	        String fileName = "read_write_hdfs_example.txt";
	        Path hdfsWritePath = new Path("/user/BigdataHUMG/HDFSexample/" + fileName);
	        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath,true);
	        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
	        bufferedWriter.write("Java API to write data in HDFS");
	        bufferedWriter.newLine();
	        bufferedWriter.close();
	        fileSystem.close();
	    }
		
		public static void readFileFromHDFS() throws IOException {
	        Configuration configuration = new Configuration();
	        configuration.set("fs.defaultFS", "hdfs://10.0.2.15");
	        FileSystem fileSystem = FileSystem.get(configuration);
	        //Create a path
	        String fileName = "read_write_hdfs_example.txt";
	        Path hdfsReadPath = new Path("/user/BigdataHUMG/HDFSexample/" + fileName);
	      
	        FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
	       
	        String out= IOUtils.toString(inputStream, "UTF-8");
	        System.out.println(out);
	       
	        inputStream.close();
	        fileSystem.close();
	    }
		
		public static void main(String[] args) {
			try {
				readFileFromHDFS();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	  
	}