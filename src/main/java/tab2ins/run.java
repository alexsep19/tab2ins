package tab2ins;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class run {
    Connection conn;
    String table, where, outfile;
    String[] exclude_flds;
    
	public run() {

	}

	private boolean fillParams(String Fn) {
		FileInputStream fis;
        Properties property = new Properties();
        String url = null, user = null, pswd = null, driver = null, ef = null;
      try{
    	fis = new FileInputStream(Fn);
        property.loadFromXML(fis);
        fis.close();

        url = property.getProperty("url", "").trim();
        user = property.getProperty("user", "").trim();
        pswd = property.getProperty("pswd", "").trim();
        driver = property.getProperty("driver", "").trim();
        table = property.getProperty("table", "").trim();
        where = property.getProperty("where", "").trim();
        outfile = property.getProperty("outfile", "our.sql").trim();
        ef = property.getProperty("exclude_flds", "").trim().trim();
        if (url.isEmpty()) throw new Exception("ϲ򴳱󣴥𞻥ntry key=\"url\">jdbc:as400:TB10</entry>");
        if (user.isEmpty()) throw new Exception("ϲ򴳱󣴥𞻥ntry key=\"user\">ER</entry>");
        if (pswd.isEmpty()) throw new Exception("ϲ򴳱󣴥𞻥ntry key=\"pswd\"></entry>");
        if (driver.isEmpty()) throw new Exception("ϲ򴳱󣴥𞻥ntry key=\"driver\">com.ibm.as400.access.AS400JDBCDriver</entry>");
        if (table.isEmpty()) throw new Exception("ϲ򴳱󣴥𞻥ntry key=\"table\">gl_etlpst</entry>");
        if (!ef.isEmpty()) exclude_flds = ef.split(",");
        else exclude_flds = new String[0];
//        System.out.println("HOST:" + url  + ", LOGIN: " + user + ", driver:" + driver);
      }catch(Exception e){
    	e.printStackTrace();
        return false;
      }
      
      try{
		Class.forName(driver);
	  } catch (ClassNotFoundException e1) {
		System.out.println("Error loading DB driver " + driver);
		e1.printStackTrace();
  		return false;
	  }

      try {
      	conn = DriverManager.getConnection(url, user, pswd);
	  }catch(SQLException e) {
		System.out.println("Error connecting to " + url);
		e.printStackTrace();
		return false;
	  }
       return true;
	}
	
	private boolean procStmt(){
	 StringBuilder ins = new StringBuilder("insert into " + table + "(");
	 try(PreparedStatement stmt = conn.prepareStatement("SELECT * FROM dwh." + table + (where.isEmpty()?"":" where " + where));
		 PrintWriter out = new PrintWriter(outfile);
	  	 ResultSet rs = stmt.executeQuery()){
		 
  		 ResultSetMetaData rsmd = rs.getMetaData();
  		 int colNum = rsmd.getColumnCount();
  		 for (int i = 1; i <= colNum; i++ ) {
  		   if (isExcludeFld(rsmd.getColumnLabel(i))) continue;
  		   ins.append(" ").append(rsmd.getColumnLabel(i)).append(",");
  		 }
  		 ins.replace(ins.length()-1, ins.length(), ")\n VALUES(");
//  		 System.out.println(ins.toString());
  		 
  		 while(rs.next()){
  			StringBuilder values = new StringBuilder(ins);
 	  		for(int i = 1; i <= colNum; i++ ) {
 	  		  if (isExcludeFld(rsmd.getColumnLabel(i))) continue;
 	  		  int type = rsmd.getColumnType(i);
 	  		  
 	  		  if (type == Types.CHAR){
 	  			if (rs.getString(i) != null) values.append("'").append(rs.getString(i).replace("'", "''")).append("',");
 	  			else values.append("''").append(",");
 	  		  }
              else if (rs.getObject(i) == null)  values.append("null").append(",");
 	  		  else if (type == Types.VARCHAR) values.append("'").append(rs.getString(i).replace("'", "''")).append("',");
 	  		  else if (type == Types.NUMERIC || type == Types.DECIMAL || type == Types.DOUBLE ||
 	  				   type == Types.FLOAT) values.append(rs.getDouble(i)).append(",");
 	  		  else if (type == Types.BIGINT || type == Types.INTEGER) values.append(rs.getLong(i)).append(",");
 	  		  else if (type == Types.DATE) values.append("'").append(new SimpleDateFormat("yyyy-MM-dd").format(rs.getDate(i))).append("',");
 	  		  else if (type == Types.TIMESTAMP)	values.append("'").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(rs.getDate(i))).append("',");
 	  		  else throw new Exception("type = " + type + " not released");
 	  		} 
 	  		values.replace(values.length()-1, values.length(), ");");
// 	  		System.out.println(values.toString());
 	  		out.println(values);
  	 	 }
  			
		 return true;
		}catch(Exception e){
		 e.printStackTrace();
		 return false;
		}
		
	}
	
	private boolean isExcludeFld(String Fn){
		for(int i = 0; i < exclude_flds.length; i++){
		  if (exclude_flds[i].trim().toUpperCase().equals(Fn.toUpperCase())) return true;
		}
		return false;
	}
	
	public static void main(String[] args) {
	 String confFileName = "conf.xml";
	 if (args.length > 0) confFileName = args[0]; 
     run r = new run();
     try{
       if (!r.fillParams(confFileName)) return;
       if (!r.procStmt()) return;
     }finally{
       if (r.conn != null) try{r.conn.close();}catch(Exception e){}
     }
       System.out.println("�������");
	} 

}
