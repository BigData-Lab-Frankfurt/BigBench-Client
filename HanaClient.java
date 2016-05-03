import java.io.BufferedWriter;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class HanaClient {


	public static void main(String[] args) throws IOException {		
		
		Connection connection = null;
		//Establish connection to HANA
		try {
			connection = DriverManager.getConnection("jdbc:sap://<IP and Port>/", "<namespace>", "<Password>");	
		} catch (SQLException e) {
			System.out.println(e);
			System.err.println("Connection failed!");
			return;
		}
		if (connection != null) {
			try {
				System.out.println("Connection successful!");
				
				//Check SQL input file and load file content (format as in file)
				File inFile = null;
				if (0 < args.length) {					
				   inFile = new File(args[0]);
				} else {
				   System.err.println("Invalid arguments count:" + args.length);
				   System.exit(0);
				}
				
				//Defining and initializing variables
				BufferedReader br = null;
				String sqlstmt_str = "";
				Statement stmt = connection.createStatement();
				String[] str_sqlstmt_parts = new String[]{};
				int i = 0;
				ResultSet resultSet = null;
				String filename_result = "";
				String filename_timestamp = "";
			    BufferedWriter bw_result = null;
			    BufferedWriter bw_timestamp = null;			    
			    List<String> time_stamp = new ArrayList<String>();				    
			    long timestamp_sec_before = 0;
			    String str_timestamp_before = "";
			    long timestamp_sec_after = 0;
			    String str_timestamp_after = "";
			    String str_time_before = "";				   
				String str_time_after = "";
				String str_time_sec_before = "";
				String str_time_sec_after = "";
				String str_duration_sec = "";
				String str_duration_min = "";
				String str_total_duration_sec = "";
				String str_total_duration_min = "";				
				double duration_sec = 0;
				double duration_min = 0;
				double total_duration_sec = 0;
				double total_duration_min = 0;				
				String str_statement = "";

			    try {
//			    	Removing line breaks, load content into a string of one line
			        String sCurrentLine;
			        br = new BufferedReader(new FileReader(inFile));
		            StringBuilder sqlstmt = new StringBuilder();
			        while ((sCurrentLine = br.readLine()) != null) {
			        	sqlstmt.append(sCurrentLine + " ");
			        }
			        sqlstmt_str = sqlstmt.toString();
			        str_sqlstmt_parts = sqlstmt_str.split(";");
			        br.close();     
			    } 
			    catch (IOException e) {
			        e.printStackTrace();
			    } 			
				
			    //Removing beginning white spaces and empty lines from array
			    ArrayList<String> list_sqlstmt_parts = new ArrayList<String>();
			    for (String s : str_sqlstmt_parts) {			    	
			    	String temp_sqlstmt = s.replaceAll("^\\s+", "");
			        if (temp_sqlstmt.matches("^[A-Z].*")) {
			        	list_sqlstmt_parts.add(temp_sqlstmt);
			        }
			    }		    		
			 			    								
				//Check if result set is needed
				if (args[2].equals("Yes")) {
					for (String sqlstmt_part : list_sqlstmt_parts) {
						//Check if file (data is now written to an array) contains SQL statement
						if (!sqlstmt_part.isEmpty()) {
							//Check if either SELECT or DDL/UPDATE/CREATE/DROP statement (only in case of SELECT a result set is needed 
							if (sqlstmt_part.startsWith("SELECT")) {
								//Make time stamp before query execution
								timestamp_sec_before = System.currentTimeMillis() / 1000;
							    str_timestamp_before = new SimpleDateFormat("yyyy_MM_dd_HH_m_ss_SSS").format(new Date());
								
							    //Execute SELECT query
								resultSet = stmt.executeQuery(sqlstmt_part);
								
								if (!resultSet.next()) {	
								    System.out.println("No data retrieved");
								} else {
									//If data was retrieved make time stamp after query execution and calculate duration
									timestamp_sec_after = System.currentTimeMillis() / 1000;
									str_timestamp_after = new SimpleDateFormat("yyyy_MM_dd_HH_m_ss_SSS").format(new Date());
									
									duration_sec = timestamp_sec_after - timestamp_sec_before;
									duration_min = duration_sec/60;	
									
									total_duration_sec += duration_sec;
									total_duration_min += duration_min;
									
									str_statement = "Time and duration for statement:\t" + sqlstmt_part;
									
									str_time_sec_before = "Timestamp in sec before execution:\t" + timestamp_sec_before;
									str_time_before = "Timestamp before execution:\t" + str_timestamp_before;
									
									str_time_sec_after = "Timestamp in sec after execution:\t" + timestamp_sec_after;
									str_time_after = "Timestamp after execution:\t" + str_timestamp_after;		
									
									str_duration_sec = "Duration in sec:\t" + duration_sec;
									str_duration_min = "Duration in min:\t" + duration_min + "\n";
									
								    ResultSetMetaData rsmd = resultSet.getMetaData();
								    
								    time_stamp.add(str_statement);
								    time_stamp.add(str_time_sec_before);
								    time_stamp.add(str_time_before);
								    time_stamp.add(str_time_sec_after);
								    time_stamp.add(str_time_after);
								    time_stamp.add(str_duration_sec);
								    time_stamp.add(str_duration_min);
								    
								    //Check if query no. should be part of file name								    								    
								    if (args[1] == "No") {
								    	filename_result = str_timestamp_before + "result.csv";
								    	filename_timestamp = str_timestamp_before + "timestamp.csv";
								    } else {
								    	String query = args[1];
								    	filename_result = query + "_" + str_timestamp_before + "_result.csv";
								    	filename_timestamp =  query + "_" + str_timestamp_before + "_timestamp.csv";
								    }
									
								    bw_result = new BufferedWriter(new FileWriter(filename_result, true));
								    								    
							        int rowcount = 0;	 				        
									int numberOfColumns = rsmd.getColumnCount();
									
									//Get column names from result set
									for (i = 1; i <= numberOfColumns; i++) {
								        String columnName = rsmd.getColumnName(i);
								        bw_result.write(columnName + "\t");
								        bw_result.flush();
								      }
									bw_result.newLine();
									bw_result.flush();
									
									//Get data and number of rows from result set
									do {
								    	rowcount++;
								        for (i = 1; i <= numberOfColumns; i++) {
								          String columnValue = resultSet.getString(i);
							              bw_result.write(columnValue + "\t");	
							              bw_result.flush();
								        }				        
								        bw_result.newLine();
								        bw_result.flush();
								      } while (resultSet.next());
									
								    bw_result.write("Number of rows: \t" + rowcount + "\n");
								    bw_result.newLine(); 
								    bw_result.flush();
									bw_result.close();
								}
							} else {
								//Make time stamp before query execution
								timestamp_sec_before = System.currentTimeMillis() / 1000;
							    str_timestamp_before = new SimpleDateFormat("yyyy_MM_dd_HH_m_ss_SSS").format(new Date());
							    
							    //Execute DDL/UPDATE/CREATE/DROP statement
								stmt.executeUpdate(sqlstmt_part);
								
								//Make time stamp after query execution and calculate duration
								timestamp_sec_after = System.currentTimeMillis() / 1000;
								str_timestamp_after = new SimpleDateFormat("yyyy_MM_dd_HH_m_ss_SSS").format(new Date());
								
								duration_sec = timestamp_sec_after - timestamp_sec_before;
								duration_min = duration_sec/60;	
								
								total_duration_sec += duration_sec;
								total_duration_min += duration_min;
								
								str_statement = "Time and duration for statement:\t" + sqlstmt_part;
								
								str_time_sec_before = "Timestamp in sec before execution:\t" + timestamp_sec_before;
								str_time_before = "Timestamp before execution:\t" + str_timestamp_before;
								
								str_time_sec_after = "Timestamp in sec after execution:\t" + timestamp_sec_after;
								str_time_after = "Timestamp after execution:\t" + str_timestamp_after;	
								
								str_duration_sec = "Duration in sec:\t" + duration_sec;
								str_duration_min = "Duration in min:\t" + duration_min + "\n";;
								
							    time_stamp.add(str_statement);
							    time_stamp.add(str_time_sec_before);
							    time_stamp.add(str_time_before);
							    time_stamp.add(str_time_sec_after);
							    time_stamp.add(str_time_after);
							    time_stamp.add(str_duration_sec);
							    time_stamp.add(str_duration_min);
							}
						} else {
							System.out.println("No sql statement in file: " + args[0]);
							System.exit(1);
						}
					}
					System.out.println("Query successful!\nData retrieved!");
					
					str_total_duration_sec = "Total duration in sec:\t" + total_duration_sec;
					str_total_duration_min = "Total duration in min:\t" + total_duration_min;
					
				    time_stamp.add(str_total_duration_sec);
				    time_stamp.add(str_total_duration_min);
					
					bw_timestamp = new BufferedWriter(new FileWriter(filename_timestamp, true));
					
					//Write time stamp and duration to file
					for( String temp_timestamp : time_stamp ) {
					    bw_timestamp.write(temp_timestamp + "\n");
					}
					
					bw_timestamp.flush();
					bw_timestamp.close();
						
					String current = System.getProperty("user.dir");
					String os_name = System.getProperty("os.name").toLowerCase();
					if (os_name.contains("windows")) {
						System.out.print("Timestamp written to file: " + current + "\\" + filename_timestamp + "\n");
						System.out.print("Query result written to file: " + current + "\\" + filename_result + "\n");	
					} else {
						System.out.print("Timestamp written to file: " + current + "/" + filename_timestamp + "\n");
						System.out.print("Query result written to file: " + current + "/" + filename_result + "\n");
					}			
				} else {		
					//Execute query without result set
					for (String sqlstmt_part : list_sqlstmt_parts) {			
						//Check if file (data is now written to an array) contains SQL statement
						if (!sqlstmt_part.isEmpty()) {
							//Check if either SELECT or DDL/UPDATE/CREATE/DROP statement (only in case of SELECT a result set is needed 
							if (sqlstmt_part.startsWith("SELECT")) {
								//Make time stamp before query execution
								timestamp_sec_before = System.currentTimeMillis() / 1000;
							    str_timestamp_before = new SimpleDateFormat("yyyy_MM_dd_HH_m_ss_SSS").format(new Date());
								
							    //Execute SELECT query
								stmt.executeQuery(sqlstmt_part);
								
								//Make time stamp after query execution and calculate duration
								timestamp_sec_after = System.currentTimeMillis() / 1000;
								str_timestamp_after = new SimpleDateFormat("yyyy_MM_dd_HH_m_ss_SSS").format(new Date());
								
								duration_sec = timestamp_sec_after - timestamp_sec_before;
								duration_min = duration_sec/60;	
								
								total_duration_sec += duration_sec;
								total_duration_min += duration_min;
								
								str_statement = "Time and duration for statement:\t" + sqlstmt_part;
								
								str_time_sec_before = "Timestamp in sec before execution:\t" + timestamp_sec_before;
								str_time_before = "Timestamp before execution:\t" + str_timestamp_before;
								
								str_time_sec_after = "Timestamp in sec after execution:\t" + timestamp_sec_after;
								str_time_after = "Timestamp after execution:\t" + str_timestamp_after;	
								
								str_duration_sec = "Duration in sec:\t" + duration_sec;
								str_duration_min = "Duration in min:\t" + duration_min + "\n";
								
							    time_stamp.add(str_statement);
							    time_stamp.add(str_time_sec_before);
							    time_stamp.add(str_time_before);
							    time_stamp.add(str_time_sec_after);
							    time_stamp.add(str_time_after);
							    time_stamp.add(str_duration_sec);
							    time_stamp.add(str_duration_min);
							} else {
								//Make time stamp before query execution
								timestamp_sec_before = System.currentTimeMillis() / 1000;
							    str_timestamp_before = new SimpleDateFormat("yyyy_MM_dd_HH_m_ss_SSS").format(new Date());
								
							    //Execute DDL/UPDATE/CREATE/DROP statement
								stmt.executeUpdate(sqlstmt_part);
								
								//Make time stamp after query execution and calculate duration
								timestamp_sec_after = System.currentTimeMillis() / 1000;
								str_timestamp_after = new SimpleDateFormat("yyyy_MM_dd_HH_m_ss_SSS").format(new Date());
								
								duration_sec = timestamp_sec_after - timestamp_sec_before;
								duration_min = duration_sec/60;		
								
								total_duration_sec += duration_sec;
								total_duration_min += duration_min;
								
								str_statement = "Time and duration for statement:\t" + sqlstmt_part;
								
								str_time_sec_before = "Timestamp in sec before execution:\t" + timestamp_sec_before;
								str_time_before = "Timestamp before execution:\t" + str_timestamp_before;
								
								str_time_sec_after = "Timestamp in sec after execution:\t" + timestamp_sec_after;
								str_time_after = "Timestamp after execution:\t" + str_timestamp_after;	
								
								str_duration_sec = "Duration in sec:\t" + duration_sec;
								str_duration_min = "Duration in min:\t" + duration_min + "\n";
								
							    time_stamp.add(str_statement);
							    time_stamp.add(str_time_sec_before);
							    time_stamp.add(str_time_before);
							    time_stamp.add(str_time_sec_after);
							    time_stamp.add(str_time_after);
							    time_stamp.add(str_duration_sec);
							    time_stamp.add(str_duration_min);
							}
						} else {
							System.out.println("No sql statement in file: " + args[0]);
							System.exit(1);
						}
					}
					
					System.out.println("Query successful!");
					
					//Check if query no. should be part of file name	
				    filename_timestamp = "";
				    if (args[1].equals("No")) {
				    	filename_timestamp = str_timestamp_before + "timestamp.csv";
				    } else {
				    	String query = args[1];
				    	filename_timestamp = query + "_" + str_timestamp_before + "_timestamp.csv";
				    }
				    
					str_total_duration_sec = "Total duration in sec:\t" + total_duration_sec;
					str_total_duration_min = "Total duration in min:\t" + total_duration_min;
					
				    time_stamp.add(str_total_duration_sec);
				    time_stamp.add(str_total_duration_min);
				    
				    bw_timestamp = new BufferedWriter(new FileWriter(filename_timestamp, true));
				    
				    //Write time stamp and duration in file
					for( String temp_timestamp : time_stamp ) {
					    bw_timestamp.write(temp_timestamp + "\n");
					}
				
					bw_timestamp.flush();	
					bw_timestamp.close();
					
					String current = System.getProperty("user.dir");					
					String os_name = System.getProperty("os.name").toLowerCase();
					if (os_name.contains("windows")) {
						System.out.print("Timestamp written to file: " + current + "\\" + filename_timestamp + "\n");
					} else {
						System.out.print("Timestamp written to file: " + current + "/" + filename_timestamp + "\n");
					}					
				}				
			
		        stmt.close();
		        connection.close();
				
			} catch (SQLException e) {
				System.out.println(e);
				System.err.println("Query failed!");
			}
		}
	}
}

