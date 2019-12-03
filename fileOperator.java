package au.edu.rmit.bdp.clustering.mapreduce;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;

public class fileOperator {

	public static void main(String[] args) throws IOException {
		
	    prepareInput();
	    splitCenter();
	    getCenters();
	    getSSE();
		groupDatapoints();
		
	}
	/**
	 * The method to extract longitude and latitude data from raw dataset
	 * @throws IOException
	 */
	private static void prepareInput() throws IOException {
		
	  String csvFile = "C:\\Users\\liqih\\Desktop\\yellow_trip.csv";
      BufferedReader br = null;
      String line = "";
      String cvsSplitBy = ",";
      int iteration = 0;

      List<String> cabList = new ArrayList<>();
      
      System.out.println("Start reading");
      try {

          br = new BufferedReader(new FileReader(csvFile));
          while ((line = br.readLine()) != null) {
          	if(iteration == 0) { 
                  iteration++;  
                  continue;
          	}
              String[] cab = line.split(cvsSplitBy);
              //Split the 2 field from raw file to add into list
              String newCab = new String(cab[5] + "," + cab [6]);
              if(Double.valueOf(cab[5])!=0 && Double.valueOf(cab[6])!=0) {
              	cabList.add(newCab);
              }                            
          }

      } catch (FileNotFoundException e) {
          e.printStackTrace();
      } catch (IOException e) {
          e.printStackTrace();
      } finally {
          if (br != null) {
              try {
                  br.close();
              } catch (IOException e) {
                  e.printStackTrace();
              }
          }
      }
      
      System.out.println("Start Writing");
      
      FileWriter writer = new FileWriter("C:\\\\Users\\\\liqih\\\\Desktop\\\\pick_up.csv");
      //Write all records in the list to a new file
      for (String cab : cabList)
      {
          String[] split = cab.split(",");
          writer.append(split[0]+","+split[1]);
          writer.append("\r\n");
      }
      writer.close();

      System.out.println("Done");
	}
		

/**
 * The method used to split all centroid from output file
 * @throws IOException
 */

      private static void splitCenter() throws IOException {
    	  
    	String csvFile = "C:\\Users\\liqih\\Desktop\\output_K_4_after_30.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
  
        List<String> centerList = new ArrayList<>();
        
        System.out.println("Start reading");
        try {
  
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
  
               String[] center = line.split(cvsSplitBy);
              //Throw the records missing required data
               if(center.length == 4) {
              	 String newCenter = new String(center[0] + "," + center [1]); 
              	 centerList.add(newCenter);
               }                      	                         
            }
  
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
        System.out.println("Start Writing");
        // Write them into new file
        FileWriter writer = new FileWriter("C:\\\\Users\\\\liqih\\\\Desktop\\\\centers.csv");
        for (String center : centerList)
        {
            String[] split = center.split(",");
            writer.append(split[0]+","+split[1]);
            writer.append("\r\n");
        }
        writer.close();
  
        System.out.println("Done");
  		
        }
       


      /**
       * The method used to extrace all centroid
       */
	
	  private static void getCenters() {
		  String csvFile = "C:\\Users\\liqih\\Desktop\\centers.csv";
	      BufferedReader br = null;
	      String line = "";
	      //String cvsSplitBy = ",";
	      String tmp = "";

	      List<String> centerList = new ArrayList<>();
	      
	      System.out.println("Start reading");
	      try {

	          br = new BufferedReader(new FileReader(csvFile));
	          while ((line = br.readLine()) != null) {
                //Get rid off repeating centroid
	            if(!line.equals(tmp)) {
	            	centerList.add(line);
	            	tmp = line;
	            }             	                         
	          }

	      } catch (FileNotFoundException e) {
	          e.printStackTrace();
	      } catch (IOException e) {
	          e.printStackTrace();
	      } finally {
	          if (br != null) {
	              try {
	                  br.close();
	              } catch (IOException e) {
	                  e.printStackTrace();
	              }
	          }
	      }	
			
		  for(int i = 0; i < centerList.size(); i++) {
			  System.out.println(centerList.get(i));
			  //Print out all centroid
		  }
	  }
	  
 /**
  * The method used to calculate the SSE
  */
	  
	  private static void getSSE() {
		  String csvFile = "C:\\Users\\liqih\\Desktop\\output_K_4_after_30.csv";
	      BufferedReader br = null;
	      String line = "";
	      String tmp = "";
	      double sum = 0;
	      double sse = 0;

	      List<String> centerList = new ArrayList<>();
	      
	      System.out.println("Start reading");
	      try {

	          br = new BufferedReader(new FileReader(csvFile));
	          while ((line = br.readLine()) != null) {
	        	  String result[] = line.split(",");
	        	  //Only calculate the SSE if there no data missing
	        	  if(result.length == 4) {	        		  		        	  
		        	  double center_x = Double.parseDouble(result[0]);
		        	  double center_y = Double.parseDouble(result[1]);
		        	  double datapoint_x = Double.parseDouble(result[2]);
		        	  double datapoint_y = Double.parseDouble(result[3]);
		        	  sse = Math.sqrt(Math.pow(datapoint_x - center_x, 2) + Math.pow(datapoint_y - center_y, 2));
		        	  sum = sum + sse;
	        	  }	        	  	        	            	                         
	          }

	      } catch (FileNotFoundException e) {
	          e.printStackTrace();
	      } catch (IOException e) {
	          e.printStackTrace();
	      } finally {
	          if (br != null) {
	              try {
	                  br.close();
	              } catch (IOException e) {
	                  e.printStackTrace();
	              }
	          }
	      }	
	      
	      System.out.println(sum);
 
		  
	  }
	  
/**
 * The method used to replace centroid data with English letter
 * Then the output can be used for drawing clusering graph	  
 * @throws IOException
 */
	  private static void groupDatapoints() throws IOException {
	    	String csvFile = "C:\\Users\\liqih\\Desktop\\output_K_8.csv";
	        BufferedReader br = null;
	        String line = "";
	        String cvsSplitBy = ",";
	  
	        List<String> centerList = new ArrayList<>();
	        
	        String newCenter="";
	        
	        System.out.println("Start reading");
	        try {
	  
	            br = new BufferedReader(new FileReader(csvFile));
	            while ((line = br.readLine()) != null) {
	  
	               String[] center = line.split(cvsSplitBy);
	              if(center.length == 4 ) {
	            	  //Check if the longitude same as the output from getCenters()
		               if(Double.parseDouble(center[0]) == -73.96115920152234) {
			              	  newCenter = new String("A" + "," + center[2] + "," +center[3]); 	              	 
			               }
			               else if(Double.parseDouble(center[0])==-73.97027559102862) {
			            	   newCenter = new String("B" + "," + center[2] + "," +center[3]);
			               }
			               else if(Double.parseDouble(center[0])==-73.78157176585262) {
			            	   newCenter = new String("C" + "," + center[2] + "," +center[3]);
			               }
			               else if(Double.parseDouble(center[0])==-73.9926267728812) {
			            	   newCenter = new String("D" + "," + center[2] + "," +center[3]);
			               }
			               else if(Double.parseDouble(center[0])==-73.87447367214169) {
			            	   newCenter = new String("E" + "," + center[2] + "," +center[3]);
			               }
			               else if(Double.parseDouble(center[0])==-73.98583482826288) {
			            	   newCenter = new String("F" + "," + center[2] + "," +center[3]);
			               }
			               else if(Double.parseDouble(center[0])==-74.00548487840342) {
			            	   newCenter = new String("G" + "," + center[2] + "," +center[3]);
			               }
			               else {
			            	   newCenter = new String("H" + "," + center[2] + "," +center[3]);		               
	                           }  
					}
	               centerList.add(newCenter);
	            }
	  
	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            if (br != null) {
	                try {
	                    br.close();
	                } catch (IOException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	        
	        System.out.println("Start Writing");
	        //Rewrite into new file
	        FileWriter writer = new FileWriter("C:\\\\Users\\\\liqih\\\\Desktop\\\\after.csv");
	        for (String center : centerList)
	        {
	            String[] split = center.split(",");
	            writer.append(split[0]+","+split[1]+","+split[2]);
	            writer.append("\r\n");
	        }
	        writer.close();
	  
	        System.out.println("Done");
	        
	  }

	
}
