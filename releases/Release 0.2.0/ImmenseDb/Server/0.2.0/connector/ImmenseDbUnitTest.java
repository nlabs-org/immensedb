import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import junit.framework.Assert;
import org.junit.Test;
import com.immensedb.jdbc.ImmenseDbCollection;

public class ImmenseDbUnitTest {

	static Connection con;
	
	@Test
	public void connectDb() throws Exception {
		con=createDnUnSQlConnection();
		Assert.assertEquals(false, con.isClosed());
		con.close();
	}
	
	@Test
	public void createDb() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		//stmt.execute("drop db DbTest");
		boolean status=stmt.execute("create db DbTest");
		
		ResultSet  rs=stmt.executeQuery("show dbs");
    	int i=0;
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
          	 if(collection.toString().equals("{\"name\":\"DbTest\"}"))
          		 i++;
        }
		Assert.assertEquals(1,i);
		status=stmt.execute("drop db DbTest");
		con.close();
	}

	@Test
	public void dropDb() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
        
		boolean status=stmt.execute("create db DbTest");
		//Assert.assertEquals(false,status);
		status=stmt.execute("drop db DbTest");
		ResultSet  rs=stmt.executeQuery("show dbs");
    	int i=0;
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
          	 if(collection.toString().equals("{\"name\":\"DbTest\"}"))
          		 i++;
        }
		Assert.assertEquals(0,i);
		
		con.close();
	}
	
	@Test
	public void dropDbWithCollection() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
	//	stmt.executeQuery("show dbs");
		status=stmt.execute("use DbTest");
		//System.out.println(status);
		status=stmt.execute("create collection DbTestCollection");
		status=stmt.execute("create collection DbTestCollection1");
		status=stmt.execute("create collection DbTestCollection2");
		//stmt.executeQuery("show collections");

		
		//status=stmt.execute("drop collection DbTestCollection");
		status=stmt.execute("drop db DbTest");
		stmt.execute("create db DbTest");
		stmt.execute("use DbTest");
		ResultSet  rs=stmt.executeQuery("show collections");
    	int i=0;
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
          	// System.out.println(collection);
          	 
          	 i++;
        }
    	status=stmt.execute("drop db DbTest");
		con.close();
		Assert.assertEquals(0,i);
	}
	
	@Test
	public void useDb() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
        
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		
		Assert.assertEquals(true,status);
		status=stmt.execute("drop db DbTest");

		con.close();
	}
	
	@Test
	public void createCollection() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
		
		ResultSet  rs=stmt.executeQuery("show collections");
    	int i=0;
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
          	// System.out.println(collection);
          	 if(collection.toString().equals("{\"name\":\"DbTestCollection\"}"))
          		 i++;
        }
		Assert.assertEquals(1,i);
		status=stmt.execute("drop collection DbTestCollection");
		status=stmt.execute("drop db DbTest");

		con.close();
	}
	
	@Test
	public void showCollections() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		//System.out.println(status);
		status=stmt.execute("create collection DbTestCollection");
		//System.out.println(status);

		ResultSet  rs=stmt.executeQuery("show collections");
    	int i=0;
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
          //	System.out.println(collection);
          	 if(collection.toString().equals("{\"name\":\"DbTestCollection\"}"))
          		 i++;
        }
    	//System.out.println("end");
		Assert.assertEquals(1,i);
		status=stmt.execute("drop collection DbTestCollection");
		status=stmt.execute("drop db DbTest");

		con.close();
	}
	
	
	@Test
	public void dropCollection() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
        
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
		status=stmt.execute("drop collection DbTestCollection");

		ResultSet  rs=stmt.executeQuery("show collections");
    	int i=0;
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
          	 if(collection.toString().equals("{\"name\":\"DbTestCollection\"}"))
          		 i++;
        }
		Assert.assertEquals(0,i);
		status=stmt.execute("drop db DbTest");
		con.close();
	}
	
	@Test
	public void insertIntoTest() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		//System.out.println(status);
		status=stmt.execute("create collection DbTestCollection");
		//System.out.println(status);

		String query="insert into DbTestCollection value {\"id\":696356,\"month\":3,\"year\":2018,\"dlrShipVersionId\":0,\"dealershipId\":0,\"dealerGroupId\":0,\"typeClassId\":4,\"baumusterId\":0,\"isActive\":\"Y\",\"countryId\":1,\"allocationRunId\":181,\"fieldName\":\"Manual Allocation Pool\",\"fieldId\":48,\"fieldCode\":\"REMAINING\",\"dc0\":0.0,\"t0\":0.0,\"ja\":0.0,\"fe\":0.0,\"mr\":0.0,\"ap\":0.0,\"my\":0.0,\"jn\":0.0,\"jl\":0.0,\"ag\":0.0,\"sp\":0.0,\"oc\":0.0,\"nv\":0.0,\"dc\":0.0,\"ja2\":0.0,\"fe2\":0.0,\"mr2\":0.0,\"ap2\":0.0,\"my2\":0.0,\"jn2\":0.0,\"jl2\":0.0,\"ag2\":0.0,\"sp2\":0.0,\"oc2\":0.0,\"nv2\":0.0,\"dc2\":0.0}";
		status=stmt.execute(query);
		//System.out.println(status);
		
		ResultSet  rs=stmt.executeQuery("select * from DbTestCollection where id=696356");
    	int i=0;
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
          	 i++;
        }
    	//System.out.println("end");
	
		status=stmt.execute("drop collection DbTestCollection");
		status=stmt.execute("drop db DbTest");

		con.close();
		Assert.assertEquals(1,i);
	}
	
	@Test
	public void insertUpdateFloatTest() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		//System.out.println(status);
		status=stmt.execute("create collection DbTestCollection");
		//System.out.println(status);

		String query="insert into DbTestCollection value {\"id\":696356,\"month\":3,\"year\":2018,\"dlrShipVersionId\":0,\"dealershipId\":0,\"dealerGroupId\":0,\"typeClassId\":4,\"baumusterId\":0,\"isActive\":\"Y\",\"countryId\":1,\"allocationRunId\":181,\"fieldName\":\"Manual Allocation Pool\",\"fieldId\":48,\"fieldCode\":\"REMAINING\",\"dc0\":0.0,\"t0\":0.0,\"ja\":0.0,\"fe\":0.0,\"mr\":0.0,\"ap\":0.0,\"my\":0.0,\"jn\":0.0,\"jl\":0.0,\"ag\":0.0,\"sp\":0.0,\"oc\":0.0,\"nv\":0.0,\"dc\":0.0,\"ja2\":0.0,\"fe2\":0.0,\"mr2\":0.0,\"ap2\":0.0,\"my2\":0.0,\"jn2\":0.0,\"jl2\":0.0,\"ag2\":0.0,\"sp2\":0.0,\"oc2\":0.0,\"nv2\":1.1,\"dc2\":2.2}";
		status=stmt.execute(query);
		//System.out.println(status);
	
		ResultSet  rs=stmt.executeQuery("select {nv2,dc2} from DbTestCollection where id=696356");
    	int i=0;
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
        //  	 System.out.println(collection);
          	 if(collection.toString().equals("{\"nv2\":1.1,\"dc2\":2.2}"))
          	 	i++;
        }

    	query="update DbTestCollection set nv2=4.4,dc2=3.3 where id=696356";
		status=stmt.execute(query);
		//System.out.println(status);
	
		 rs=stmt.executeQuery("select {nv2,dc2} from DbTestCollection where id=696356");
    
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
          	// System.out.println(collection);
          	 if(collection.toString().equals("{\"nv2\":2.2,\"dc2\":3.3}"))
          	 	i++;
        }
    	
	
		status=stmt.execute("drop collection DbTestCollection");
		status=stmt.execute("drop db DbTest");

		con.close();
		Assert.assertEquals(1,i);
	}
	
	@Test
	public void selectTest() throws Exception {
		con=createDnUnSQlConnection();
		Statement stmt=con.createStatement();
		int i=0;
		//try{	
			
			boolean status=stmt.execute("create db DbTest");
			status=stmt.execute("use DbTest");
			//System.out.println(status);
			status=stmt.execute("create collection DbTestCollection");
			//System.out.println(status);
	
			String query="insert into DbTestCollection value {\"id\":696356,\"month\":3,\"year\":2018,\"dlrShipVersionId\":0,\"dealershipId\":0,\"dealerGroupId\":0,\"typeClassId\":4,\"baumusterId\":0,\"isActive\":\"Y\",\"countryId\":1,\"allocationRunId\":181,\"fieldName\":\"Manual Allocation Pool\",\"fieldId\":48,\"fieldCode\":\"REMAINING\",\"dc0\":0.0,\"t0\":0.0,\"ja\":0.0,\"fe\":0.0,\"mr\":0.0,\"ap\":0.0,\"my\":0.0,\"jn\":0.0,\"jl\":0.0,\"ag\":0.0,\"sp\":0.0,\"oc\":0.0,\"nv\":0.0,\"dc\":0.0,\"ja2\":0.0,\"fe2\":0.0,\"mr2\":0.0,\"ap2\":0.0,\"my2\":0.0,\"jn2\":0.0,\"jl2\":0.0,\"ag2\":0.0,\"sp2\":0.0,\"oc2\":0.0,\"nv2\":1,\"dc2\":2}";
			status=stmt.execute(query);
			//System.out.println(status);
		//MySQL server version for the right syntax to use near '= 101
			 
			ResultSet  rs=stmt.executeQuery("select {nv2,dc2} from DbTestCollection where id=696356 and nv2=1 and dc2=2");
	    	
	    	while(rs.next()) {
	          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
	         //	System.out.println(collection);
	          	 if(collection.toString().equals("{\"nv2\":1,\"dc2\":2}"))
	          	 	i++;
	        }
	    	//System.out.println("end");
		
	    	
	    	query="insert into DbTestCollection value {\"id\":696356,\"month\":3,\"year\":2018,\"dlrShipVersionId\":0,\"dealershipId\":0,\"dealerGroupId\":0,\"typeClassId\":4,\"baumusterId\":0,\"isActive\":\"Y\",\"countryId\":1,\"allocationRunId\":181,\"fieldName\":\"Manual Allocation Pool\",\"fieldId\":48,\"fieldCode\":\"REMAINING\",\"dc0\":0.0,\"t0\":0.0,\"ja\":0.0,\"fe\":0.0,\"mr\":0.0,\"ap\":0.0,\"my\":0.0,\"jn\":0.0,\"jl\":0.0,\"ag\":0.0,\"sp\":0.0,\"oc\":0.0,\"nv\":0.0,\"dc\":0.0,\"ja2\":0.0,\"fe2\":0.0,\"mr2\":0.0,\"ap2\":0.0,\"my2\":0.0,\"jn2\":0.0,\"jl2\":0.0,\"ag2\":0.0,\"sp2\":0.0,\"oc2\":0.0,\"nv2\":1.5,\"dc2\":2.0}";
			status=stmt.execute(query);
		//	System.out.println("Insert "+status);
		
			rs=stmt.executeQuery("select {nv2,dc2} from DbTestCollection where id=696356 and nv2=1.5 and dc2=2.0");
	    	
	    	while(rs.next()) {
	          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
	         //	System.out.println(collection);
	          	 if(collection.toString().equals("{\"nv2\":1.5,\"dc2\":2.0}"))
	          	 	i++;
	        }
           rs=stmt.executeQuery("select {nv2,dc2} from DbTestCollection where id=696356 and nv2>1 ");
	    	
	    	while(rs.next()) {
	          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
	         	//System.out.println(collection);
	          	 if(collection.toString().equals("{\"nv2\":1.5,\"dc2\":2.0}"))
	          	 	i++;
	        }
	    
	    	   rs=stmt.executeQuery("select {nv2,dc2} from DbTestCollection where id=696356 and nv2>-2 ");
		    	
		    	while(rs.next()) {
		          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
		         	//System.out.println(collection);
		          	 if(collection.toString().equals("{\"nv2\":1.5,\"dc2\":2.0}"))
		          	 	i++;
		        }
		    
	    	
	    	 rs=stmt.executeQuery("select {nv2,dc2} from DbTestCollection limit 1,1");
		    	
		    	while(rs.next()) {
		          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
		         	//System.out.println("d "+collection);
		          	// if(collection.toString().equals("{\"nv2\":1.5,\"dc2\":2.0}"))
		          	 	i++;
		        }
//	    }
//    	catch(Exception e){
//    		
//    	}
    	
		stmt.execute("drop collection DbTestCollection");
		stmt.execute("drop db DbTest");

		con.close();
		Assert.assertEquals(5,i);
	}
	
	@Test
	public void updateTest() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		
		PreparedStatement pstmt=con.prepareStatement("update DbTestCollection set nv2=?,dc2=? where id=?");
		
		
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		//System.out.println(status);
		status=stmt.execute("create collection DbTestCollection");
		//System.out.println(status);

		String query="insert into DbTestCollection value {\"id\":696356,\"month\":3,\"year\":2018,\"dlrShipVersionId\":0,\"dealershipId\":0,\"dealerGroupId\":0,\"typeClassId\":4,\"baumusterId\":0,\"isActive\":\"Y\",\"countryId\":1,\"allocationRunId\":181,\"fieldName\":\"Manual Allocation Pool\",\"fieldId\":48,\"fieldCode\":\"REMAINING\",\"dc0\":0.0,\"t0\":0.0,\"ja\":0.0,\"fe\":0.0,\"mr\":0.0,\"ap\":0.0,\"my\":0.0,\"jn\":0.0,\"jl\":0.0,\"ag\":0.0,\"sp\":0.0,\"oc\":0.0,\"nv\":0.0,\"dc\":0.0,\"ja2\":0.0,\"fe2\":0.0,\"mr2\":0.0,\"ap2\":0.0,\"my2\":0.0,\"jn2\":0.0,\"jl2\":0.0,\"ag2\":0.0,\"sp2\":0.0,\"oc2\":0.0,\"nv2\":0.0,\"dc2\":0.0}";
		status=stmt.execute(query);
		//System.out.println(status);
		
		
		query="update DbTestCollection set nv2=2,dc2=3 where id=696356";
		status=stmt.execute(query);
//		
//		pstmt.setInt(1,2);
//		pstmt.setInt(2,3);
//		pstmt.setInt(3,696356);
//		
//		int st=pstmt.executeUpdate();
//		
		ResultSet  rs=stmt.executeQuery("select {nv2,dc2} from DbTestCollection where id=696356");
    	int i=0;
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
          	// System.out.println(collection);
          	 if(collection.toString().equals("{\"nv2\":2,\"dc2\":3}"));
          	 	i++;
        }
    	//System.out.println("end");
	
    	
    	query="insert into DbTestCollection value {\"id\":696357,\"month\":3,\"year\":2018,\"dlrShipVersionId\":0,\"dealershipId\":0,\"dealerGroupId\":0,\"typeClassId\":4,\"baumusterId\":0,\"isActive\":\"Y\",\"countryId\":1,\"allocationRunId\":181,\"fieldName\":\"Manual Allocation Pool\",\"fieldId\":48,\"fieldCode\":\"REMAINING\",\"dc0\":0.0,\"t0\":0.0,\"ja\":0.0,\"fe\":0.0,\"mr\":0.0,\"ap\":0.0,\"my\":0.0,\"jn\":0.0,\"jl\":0.0,\"ag\":0.0,\"sp\":0.0,\"oc\":0.0,\"nv\":0.0,\"dc\":0.0,\"ja2\":0.0,\"fe2\":0.0,\"mr2\":0.0,\"ap2\":0.0,\"my2\":0.0,\"jn2\":0.0,\"jl2\":0.0,\"ag2\":0.0,\"sp2\":0.0,\"oc2\":0.0,\"nv2\":\"0.0\",\"dc2\":\"0.0\"}";
		status=stmt.execute(query);
		//System.out.println(status);
		
		query="update DbTestCollection set nv2='2',dc2='3' where id=696357";
		status=stmt.execute(query);
		

        rs=stmt.executeQuery("select {nv2,dc2} from DbTestCollection where id=696357");


    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
          //	 System.out.println(collection);
          	 if(collection.toString().equals("{\"nv2\":\"2\",\"dc2\":\"3\"}"));
          	 	i++;
        }
    	
		status=stmt.execute("drop collection DbTestCollection");
		status=stmt.execute("drop db DbTest");

		con.close();
		Assert.assertEquals(2,i);
	}
	
	
	@Test
	public void arraySelectTest() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		//System.out.println(status);
		status=stmt.execute("create collection DbTestCollection");
		//System.out.println(status);

		PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}]}");
		
		//System.out.println("start1 "+(new Date()));
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,2);
		pstmt.setInt(6,3);
    	status=pstmt.execute();
	
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
    	
    	
    	// for(int ii=1 ;ii<=50;ii++){
	    	pstmt.setInt(1,696358);
	    	pstmt.setString(2,"\"mita\"");
			pstmt.setString(3,"\"mita123\"");
			pstmt.setInt(4,2);
			pstmt.setInt(5,9);
			pstmt.setInt(6,3);
	    	status=pstmt.execute();
    	// }
    	 //System.out.println("start "+(new Date()));
    	ResultSet  rs=stmt.executeQuery("select {id,userId,password,access.accessId} from DbTestCollection where access.accessId=2"); 
    	int i=0;
    	
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
           //  System.out.println(collection);
          	// if(collection.toString().equals("{\"sum\":15}"))
          	 	i++;
        }
    	// System.out.println("End "+(new Date()));
    	
		status=stmt.execute("drop collection DbTestCollection");
		status=stmt.execute("drop db DbTest");

		con.close();
		Assert.assertEquals(2,i);
	}
	
	@Test
	public void insertIntoArrayTest() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		//System.out.println(status);
		status=stmt.execute("create collection DbTestCollection");
		//System.out.println(status);

		PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}]}");
		
		//System.out.println("start1 "+(new Date()));
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,2);
		pstmt.setInt(6,3);
    	status=pstmt.execute();
	
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
    	
    	
    	// for(int ii=1 ;ii<=50;ii++){
	    	pstmt.setInt(1,696358);
	    	pstmt.setString(2,"\"mita\"");
			pstmt.setString(3,"\"mita123\"");
			pstmt.setInt(4,2);
			pstmt.setInt(5,9);
			pstmt.setInt(6,3);
	    	status=pstmt.execute();
    	// }
    	 //System.out.println("start "+(new Date()));
	    	
    	pstmt=con.prepareStatement("insert into DbTestCollection.access value {\"accessId\":77,\"isActive\":\"Y\"} where id=696357 and  userId='pravakar'");
    	status=pstmt.execute();	
    	
    	//pstmt=con.prepareStatement("delete from DbTestCollection.access where access.accessId=77 and id=696357");
    	//status=pstmt.execute();	
	    //delete from 	DbTestCollection.access where access.accessId=77 and id=696357
    	//update DbTestCollection.access set isActive='N' where access.accessId=77 and id=696357
    	
	    	
    	ResultSet  rs=stmt.executeQuery("select * from DbTestCollection where access.accessId=77"); 
    	int i=0;
    	
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
            // System.out.println(collection);
          	// if(collection.toString().equals("{\"sum\":15}"))
          	 	i++;
        }
    	// System.out.println("End "+(new Date()));
    	
		status=stmt.execute("drop collection DbTestCollection");
		status=stmt.execute("drop db DbTest");

		con.close();
		Assert.assertEquals(1,i);
	}
	
	

	@Test
	public void deleteUpdateIntoArrayTest() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		//status=stmt.execute("use DbTest");
		//status=stmt.execute("drop collection DbTestCollection");

		//System.out.println(status);
		status=stmt.execute("create collection DbTestCollection");
		//System.out.println(status);

		PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"address\":?,\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}]}");
		
		//System.out.println("start1 "+(new Date()));
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setString(4,"{\"city\":\"dibakar123\",\"pin\":735221}");

		pstmt.setInt(5,1);
		pstmt.setInt(6,2);
		pstmt.setInt(7,3);
    	status=pstmt.execute();
	
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setString(4,"{\"city\":\"dibakar123\",\"pin\":735221}");

		pstmt.setInt(5,1);
		pstmt.setInt(6,6);
		pstmt.setInt(7,5);
    	status=pstmt.execute();
    	
    	
    	// for(int ii=1 ;ii<=50;ii++){
	    	pstmt.setInt(1,696358);
	    	pstmt.setString(2,"\"mita\"");
			pstmt.setString(3,"\"mita123\"");
			pstmt.setString(4,"{\"city\":\"dibakar123\",\"pin\":735221}");

			pstmt.setInt(5,2);
			pstmt.setInt(6,99);
			pstmt.setInt(7,3);
	    	status=pstmt.execute();
    	// }
    	 //System.out.println("start "+(new Date()));
	    	
    	pstmt=con.prepareStatement("insert into DbTestCollection.access value {\"accessId\":77,\"isActive\":\"Y\"} where id=696357 and  userId='pravakar'");
    	status=pstmt.execute();	
    	
    	//pstmt=con.prepareStatement("delete from DbTestCollection.access where access.accessId=77 and id=696357");
    	//status=pstmt.execute();	
	    //delete from 	DbTestCollection.access where access.accessId=77 and id=696357
    	//update DbTestCollection.access set isActive='N' where access.accessId=77 and id=696357
        pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"address\":?}");
		
		//System.out.println("start1 "+(new Date()));
		pstmt.setInt(1,696366);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setString(4,"{\"city\":\"dibakar123\",\"pin\":735222}");

		status=pstmt.execute();
	    	
		pstmt=con.prepareStatement("insert into DbTestCollection.access value {\"accessId\":77,\"isActive\":\"Y\"} where id=696366 and  userId='dibakar'");
    	status=pstmt.execute();	
		
    	pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"address\":?}");
    	pstmt.setInt(1,696368);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setString(4,"{\"city\":\"dibakar123\",\"pin\":735221}");
		status=pstmt.execute();
    	
    	ResultSet  rs=stmt.executeQuery("select * from DbTestCollection where access.accessId=99"); 
    	int i=0;
    	
    	
    	ResultSetMetaData rsm=rs.getMetaData();
    	// System.out.println(rsm.getColumnCount());
    	// System.out.println(rsm.getColumnName(1));
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
            // System.out.println(" step 1 "+collection);
          	// if(collection.toString().equals("{\"sum\":15}"))
          	 	i++;
        }
    	// System.out.println("End "+(new Date()));
    
    
    	
    	 
    	pstmt=con.prepareStatement("update DbTestCollection.access set isActive='N' where id=696358 and access.accessId=99");
         
     	status=pstmt.execute();	
 //	
     	
     	 rs=stmt.executeQuery("select * from DbTestCollection where id=696358 and access.accessId=99 and access.isActive='N'" ); 
      
     	 while(rs.next()) {
            	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
           //   System.out.println(" step 2 "+collection);
            	// if(collection.toString().equals("{\"sum\":15}"))
            	 	i++;
          }
     	 
     	pstmt=con.prepareStatement("delete from DbTestCollection.access where id=696358 and access.accessId=99");
     	 
     	status=pstmt.execute();	
 //	
     	
     	 rs=stmt.executeQuery("select * from DbTestCollection where id=696358  and access.accessId=99"); 
      
     	 while(rs.next()) {
            	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
               System.out.println(" step 3"+collection);
            	// if(collection.toString().equals("{\"sum\":15}"))
            	 	i++;
          }
		status=stmt.execute("drop collection DbTestCollection");
		status=stmt.execute("drop db DbTest");

		con.close();
		Assert.assertEquals(2,i);
	}

	
	@Test
	public void selectAndAccessArray() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status;
		//stmt.execute("drop db DbTest");
		//status=stmt.execute("if not exist create db DbTest");
		status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
	
		
        PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"access2\":[77,\"Y\",55],\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":55,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}],\"createDate\":DATE('2018-03-10 10:10:10.444','YYYY-MM-DD HH:MM:SS.Z'),\"uppdateDate\":TIMESTAMP('2018-03-10 10:10:10.444','YYYY-MM-DD HH:MM:SS.ZZ'),\"address\":{\"cityCode\":?,\"isActive\":\"Y\"},\"adharNo\":NULL,\"isSingle\":TRUE,\"isLocal\":FALSE}");
		
		
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,3);
    	status=pstmt.execute();
    	//System.out.println("Query "+pstmt);
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	pstmt.setInt(1,696358);
    	pstmt.setString(2,"\"Mita\"");
		pstmt.setString(3,"\"Mita123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	
    	
    	
    	
    	Statement stmnt=con.createStatement();
    	
	
    	//{address,adharNo,isSingle,isLocal,createDate,uppdateDate}
       	ResultSet  rs=stmnt.executeQuery("select {access[2],access2[2]} from DbTestCollection where access[2].accessId=6");// where address.cityCode=5"); 
       	int i=0;

   	    while(rs.next()) {
         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
         	 if(collection.toString().equals("{\"access[2]\":{\"accessId\":6,\"isActive\":\"N\"},\"access2[2]\":55}"))
         		 i++;
             //System.out.println("CM 1"+collection);
         	

        }
   		
    	status=stmt.execute("drop db DbTest");
    	
    	con.close();
		Assert.assertEquals(3,i);

	}
	
	@Test
	public void updateDeleteArrayObject() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status;
		//stmt.execute("drop db DbTest");
		//status=stmt.execute("if not exist create db DbTest");
		status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
	
		
        PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"access2\":[77,\"Y\",55],\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":55,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}],\"createDate\":DATE('2018-03-10 10:10:10.444','YYYY-MM-DD HH:MM:SS.Z'),\"uppdateDate\":TIMESTAMP('2018-03-10 10:10:10.444','YYYY-MM-DD HH:MM:SS.ZZ'),\"address\":{\"cityCode\":?,\"isActive\":\"Y\"},\"adharNo\":NULL,\"isSingle\":TRUE,\"isLocal\":FALSE}");
		
		
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,3);
    	status=pstmt.execute();
    
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	pstmt.setInt(1,696358);
    	pstmt.setString(2,"\"Mita\"");
		pstmt.setString(3,"\"Mita123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		

    	Statement stmnt=con.createStatement();
    	
  	
    	stmnt.execute("update DbTestCollection set userId='DnUpdate',access2=NULL where id=696356");
  	stmnt.execute("update DbTestCollection set userId='DnUpdate',access2=[] where id=696357");
  	stmnt.execute("update DbTestCollection set userId='DnUpdate',access2=55 where id=696356");
	stmnt.execute("update DbTestCollection set userId='DnUpdate',access2=NULL where id=696358");

       	ResultSet  rs=stmnt.executeQuery("select {id,access2} from DbTestCollection");// where address.cityCode=5"); 
       	int i=0;

   	    while(rs.next()) {
         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
         	 if(collection.toString().equals("{\"id\":696356,\"access2\":55}")
         		|| collection.toString().equals("{\"id\":696357,\"access2\":[]}")
         		|| collection.toString().equals("{\"id\":696358,\"access2\":NULL}"))
         		 i++;
         }
   		
    	status=stmt.execute("drop db DbTest");
    	
    	con.close();
		Assert.assertEquals(3,i);

	}
		

	
	@Test
	public void insertupdateDeleteIntoArrayObject() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status;
		//stmt.execute("drop db DbTest");
		//status=stmt.execute("if not exist create db DbTest");
		status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
	
		
        PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"access2\":[77,\"Y\",55],\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":55,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}],\"createDate\":DATE('2018-03-10 10:10:10.444','YYYY-MM-DD HH:MM:SS.Z'),\"uppdateDate\":TIMESTAMP('2018-03-10 10:10:10.444','YYYY-MM-DD HH:MM:SS.ZZ'),\"address\":{\"cityCode\":?,\"isActive\":\"Y\"},\"adharNo\":NULL,\"isSingle\":TRUE,\"isLocal\":FALSE}");
		
		
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,3);
    	status=pstmt.execute();
    //	System.out.println("Query "+pstmt);
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	pstmt.setInt(1,696358);
    	pstmt.setString(2,"\"Mita\"");
		pstmt.setString(3,"\"Mita123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
	 	
    	Statement stmnt=con.createStatement();
       	int i=0;

 	    stmnt.execute("delete from DbTestCollection.access where id=696356");// and access.accessId=6");
   	    stmnt.execute("update DbTestCollection.access set accessId=77 where id=696357 and access.accessId=55");
        stmnt.execute("insert into DbTestCollection.access value {\"dd\":55} where id=696358 and access.accessId=1");//OK
 	 
        stmnt.execute("insert into DbTestCollection.access2 value 88 where id=696356 and access2=77");//OK
   	    stmnt.execute("update DbTestCollection.access2 set access2=99 where id=696357 and access2=77");
   	    stmnt.execute("delete 77 from DbTestCollection.access2 where id=696358 and access2=77");
        //insert,delete, update "access2":[77,"Y",55] pending
        
        ResultSet  rs=stmnt.executeQuery("select {id,access,access2} from DbTestCollection");// where address.cityCode=5"); 
	    while(rs.next()) {
      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
      	 if(collection.toString().equals("{\"id\":696356,\"access\":[],\"access2\":[88,77,\"Y\",55]}")
          		|| collection.toString().equals("{\"id\":696357,\"access\":[{\"accessId\":1,\"isActive\":\"Y\"},{\"accessId\":77,\"isActive\":\"Y\"},{\"accessId\":6,\"isActive\":\"N\"}],\"access2\":[99,\"Y\",55]}")
          		|| collection.toString().equals("{\"id\":696358,\"access\":[{\"dd\":55},{\"accessId\":1,\"isActive\":\"Y\"},{\"accessId\":55,\"isActive\":\"Y\"},{\"accessId\":6,\"isActive\":\"N\"}],\"access2\":[\"Y\",55]}"))
      		 i++;
       //   System.out.println("CM 1"+collection);
        }
    	status=stmt.execute("drop db DbTest");
    	
    	con.close();
		Assert.assertEquals(3,i);

	}	
	@Test
	public void insertupdateDeleteIntoOnlyObject() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status;
	//	stmt.execute("drop db DbTest");
		//status=stmt.execute("if not exist create db DbTest");
		status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
	
		
        PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"access2\":[77,\"Y\",55],\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":55,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}],\"createDate\":DATE('2018-03-10 10:10:10.444','YYYY-MM-DD HH:MM:SS.Z'),\"uppdateDate\":TIMESTAMP('2018-03-10 10:10:10.444','YYYY-MM-DD HH:MM:SS.ZZ'),\"address\":{\"cityCode\":?,\"isActive\":\"Y\"},\"adharNo\":NULL,\"isSingle\":TRUE,\"isLocal\":FALSE}");
		
		
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,3);
    	status=pstmt.execute();
    	//System.out.println("Query "+pstmt);
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	pstmt.setInt(1,696358);
    	pstmt.setString(2,"\"Mita\"");
		pstmt.setString(3,"\"Mita123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
	 	
    	Statement stmnt=con.createStatement();
       	int i=0;

 	    stmnt.execute("delete cityCode from DbTestCollection.address  where id=696356 and address.cityCode=3");
   	    stmnt.execute("update DbTestCollection.address set cityCode=77 where id=696357 and address.cityCode=5");//OK
       stmnt.execute("insert into DbTestCollection.address value \"dd\":55 where id=696358 and address.cityCode=5");//OK
 	 

        ResultSet  rs=stmnt.executeQuery("select {id,address} from DbTestCollection");// where address.cityCode=5"); 
	    while(rs.next()) {
      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
      	 if(collection.toString().equals("{\"id\":696356,\"address\":{\"isActive\":\"Y\"}}")
          		|| collection.toString().equals("{\"id\":696357,\"address\":{\"cityCode\":77,\"isActive\":\"Y\"}}")
          		|| collection.toString().equals("{\"id\":696358,\"address\":{\"dd\":55,\"cityCode\":5,\"isActive\":\"Y\"}}"))
      		 i++;
         // System.out.println("CM 1"+collection);
        }
    	status=stmt.execute("drop db DbTest");
    	
    	con.close();
		Assert.assertEquals(3,i);

	}
		
		
	
	@Test
	public void singleConnectionTest() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
	//	stmt.executeQuery("show dbs");
		status=stmt.execute("use DbTest");
		//System.out.println(status);
		status=stmt.execute("create collection DbTestCollection");
		//stmt.executeQuery("show collections");


		PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}]}");
		
		//System.out.println("start1 "+(new Date()));
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,2);
		pstmt.setInt(6,3);
    	status=pstmt.execute();
	
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
    	

   	 for(int ii=1 ;ii<=52;ii++){
	    	pstmt.setInt(1,ii);
	    	pstmt.setString(2,"\"mita\"");
			pstmt.setString(3,"\"mita123\"");
			pstmt.setInt(4,2);
			pstmt.setInt(5,9);
			pstmt.setInt(6,3);
	    	status=pstmt.execute();
   	 }
   	 //System.out.println("start "+(new Date()));
	 
//	    	pstmt=con.prepareStatement("select * from DbTestCollection");
//   	ResultSet  rs=pstmt.executeQuery(); 
   	
   	Statement stmnt=con.createStatement();
   	ResultSet  rs=stmnt.executeQuery("select * from DbTestCollection"); 
   	int i=0;
   	int j=0;
   	while(rs.next()) {
         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
           //System.out.println("RS 1"+collection);
         	// if(collection.toString().equals("{\"sum\":15}"))
         	 	i++;
         	 	if(i>10)
         	 		break;
       }
   	// System.out.println("End "+(new Date()));
   
   	
   	ResultSet   rs1=stmnt.executeQuery("select * from DbTestCollection"); 
    	 //i=0;
    	
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
           // System.out.println("RS 1"+collection);
          	// if(collection.toString().equals("{\"sum\":15}"))
          	 	i++;
          	if(i>25)
         	 		break;
        }
    	while(rs1.next()) {
         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs1.getObject(1);
          // System.out.println("RS 2"+collection);
         	j++;
       }
    	while(rs.next()) {
         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
           //System.out.println("RS 1"+collection);
         	// if(collection.toString().equals("{\"sum\":15}"))
         	 	i++;
         	
       }
		status=stmt.execute("drop collection DbTestCollection");
		status=stmt.execute("drop db DbTest");

		con.close();
		Assert.assertEquals(54,i);
		Assert.assertEquals(54,j);
	}
	//show all test
	//
	
	@Test
	public void orderByTest() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
		int i=0;
		try{
		
			PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"month\":3,\"year\":2018,\"dlrShipVersionId\":0,\"dealershipId\":0,\"dealerGroupId\":0,\"typeClassId\":4,\"baumusterId\":0,\"isActive\":\"Y\",\"countryId\":1,\"allocationRunId\":181,\"fieldName\":\"Manual Allocation Pool\",\"fieldId\":48,\"fieldCode\":\"REMAINING\",\"dc0\":0.0,\"t0\":0.0,\"ja\":0.0,\"fe\":0.0,\"mr\":0.0,\"ap\":0.0,\"my\":0.0,\"jn\":0.0,\"jl\":0.0,\"ag\":0.0,\"sp\":0.0,\"oc\":0.0,\"nv\":0.0,\"dc\":0.0,\"ja2\":0.0,\"fe2\":0.0,\"mr2\":0.0,\"ap2\":0.0,\"my2\":0.0,\"jn2\":0.0,\"jl2\":0.0,\"ag2\":0.0,\"sp2\":0.0,\"oc2\":0.0,\"nv2\":?,\"dc2\":?}");
			pstmt.setInt(1,696356);
			pstmt.setInt(2,1);
			pstmt.setInt(3,2);
			
			
	    	status=pstmt.execute();
		
			pstmt.setInt(1,6);
	    	pstmt.setInt(2,4);
			pstmt.setInt(3,6);
			
	    	status=pstmt.execute();
		
			pstmt.setInt(1,26);
	    	pstmt.setInt(2,5);
			pstmt.setInt(3,37);
			
	    	status=pstmt.execute();
	
			pstmt.setInt(1,16);
	    	pstmt.setInt(2,5);
			pstmt.setInt(3,17);
			
	    	status=pstmt.execute();
	
			pstmt.setInt(1,16);
	    	pstmt.setInt(2,15);
			pstmt.setInt(3,7);
			
	    	status=pstmt.execute();
	
			pstmt.setInt(1,16);
	    	pstmt.setInt(2,15);
			pstmt.setInt(3,87);
			
	    	status=pstmt.execute();
	    	
	    	ResultSet  rs=stmt.executeQuery("select {dc2} from DbTestCollection order by dc2");
	    	int pos=0;
	    	while(rs.next()) {
	          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
	        //  System.out.println(collection +" "+pos);
	          	if(pos==0 && collection.toString().equals("{\"dc2\":2}"))
	          	 	i++;
	          	else if(pos==1 && collection.toString().equals("{\"dc2\":6}"))
	          	 	i++;
	          	else if(pos==2 && collection.toString().equals("{\"dc2\":7}"))
	          	 	i++;
	          	else if(pos==3 && collection.toString().equals("{\"dc2\":17}"))
	          	 	i++;
	          	else if(pos==4 && collection.toString().equals("{\"dc2\":37}"))
	          	 	i++;
	          	else if(pos==5 && collection.toString().equals("{\"dc2\":87}"))
	          	 	i++;
	
	          	pos++;
	        }
	    	
	    	
	    	 rs=stmt.executeQuery("select {nv2,dc2} from DbTestCollection order by nv2,dc2");
		    	pos=0;
		    	while(rs.next()) {
		          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
		          //	 System.out.println(collection +" "+pos);
		          	if(pos==0 && collection.toString().equals("{\"nv2\":1,\"dc2\":2}"))
		          	 	i++;
		          	else if(pos==1 && collection.toString().equals("{\"nv2\":4,\"dc2\":6}"))
		          	 	i++;
		          	else if(pos==2 && collection.toString().equals("{\"nv2\":5,\"dc2\":17}"))
		          	 	i++;
		          	else if(pos==3 && collection.toString().equals("{\"nv2\":5,\"dc2\":37}"))
		          	 	i++;
		          	else if(pos==4 && collection.toString().equals("{\"nv2\":15,\"dc2\":7}"))
		          	 	i++;
		          	else if(pos==5 && collection.toString().equals("{\"nv2\":15,\"dc2\":87}"))
		          	 	i++;
//		
		          	pos++;
		        }
		    	
	    	
	    	
		}
		finally{
			status=stmt.execute("drop collection DbTestCollection");
			status=stmt.execute("drop db DbTest");
		}
		con.close();
		Assert.assertEquals(12,i);
	}
	

	@Test
	public void miltpleSortingTest() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
		int i=0;
		try{
		
			PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"month\":3,\"year\":2018,\"dlrShipVersionId\":0,\"dealershipId\":0,\"dealerGroupId\":0,\"typeClassId\":4,\"baumusterId\":0,\"isActive\":\"Y\",\"countryId\":1,\"allocationRunId\":181,\"fieldName\":\"Manual Allocation Pool\",\"fieldId\":48,\"fieldCode\":\"REMAINING\",\"dc0\":0.0,\"t0\":0.0,\"ja\":0.0,\"fe\":0.0,\"mr\":0.0,\"ap\":0.0,\"my\":0.0,\"jn\":0.0,\"jl\":0.0,\"ag\":0.0,\"sp\":0.0,\"oc\":0.0,\"nv\":0.0,\"dc\":0.0,\"ja2\":0.0,\"fe2\":0.0,\"mr2\":0.0,\"ap2\":0.0,\"my2\":0.0,\"jn2\":0.0,\"jl2\":0.0,\"ag2\":0.0,\"sp2\":?,\"oc2\":?,\"nv2\":?,\"dc2\":?}");
			pstmt.setInt(1,696356);
			pstmt.setInt(2,1);
			pstmt.setInt(3,2);
			pstmt.setInt(4,2);
			pstmt.setInt(5,2);
			
			
	    	status=pstmt.execute();
		
			pstmt.setInt(1,6);
	    	pstmt.setInt(2,4);
			pstmt.setInt(3,6);
			pstmt.setInt(4,2);
			pstmt.setInt(5,22);
			
	    	status=pstmt.execute();
		
			pstmt.setInt(1,26);
	    	pstmt.setInt(2,5);
			pstmt.setInt(3,37);
			pstmt.setInt(4,3);
			pstmt.setInt(5,22);
		
			status=pstmt.execute();
			
			pstmt.setInt(1,26);
	    	pstmt.setInt(2,5);
			pstmt.setInt(3,37);
			pstmt.setInt(4,2);
			pstmt.setInt(5,22);
			
			status=pstmt.execute();
			
			pstmt.setInt(1,26);
	    	pstmt.setInt(2,5);
			pstmt.setInt(3,37);
			pstmt.setInt(4,1);
			pstmt.setInt(5,22);
			
			
	    	status=pstmt.execute();
	
			pstmt.setInt(1,16);
	    	pstmt.setInt(2,5);
			pstmt.setInt(3,17);
			pstmt.setInt(4,2);
			pstmt.setInt(5,22);
			
	    	status=pstmt.execute();
	
			pstmt.setInt(1,16);
	    	pstmt.setInt(2,15);
			pstmt.setInt(3,7);
			pstmt.setInt(4,9);
			pstmt.setInt(5,22);
			
	    	status=pstmt.execute();
	
			pstmt.setInt(1,16);
	    	pstmt.setInt(2,15);
			pstmt.setInt(3,87);
			pstmt.setInt(4,7);
			pstmt.setInt(5,7);
			
			status=pstmt.execute();
			
			pstmt.setInt(1,16);
	    	pstmt.setInt(2,15);
			pstmt.setInt(3,87);
			pstmt.setInt(4,7);
			pstmt.setInt(5,2);
			
			status=pstmt.execute();
			
			pstmt.setInt(1,16);
	    	pstmt.setInt(2,15);
			pstmt.setInt(3,87);
			pstmt.setInt(4,7);
			pstmt.setInt(5,1);
			
			
	    	status=pstmt.execute();
	    	
	    	ResultSet  rs=stmt.executeQuery("select {dc2} from DbTestCollection order by dc2");
	    	int pos=0;
	    	while(rs.next()) {
	          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
	          // System.out.println(collection +" "+pos);
	          	if(pos==0 && collection.toString().equals("{\"dc2\":1}"))
	          	 	i++;
	          	else if(pos==1 && collection.toString().equals("{\"dc2\":2}"))
	          	 	i++;
	          	else if(pos==2 && collection.toString().equals("{\"dc2\":2}"))
	          	 	i++;
	          	else if(pos==3 && collection.toString().equals("{\"dc2\":7}"))
	          	 	i++;
	          	else if(pos==4 && collection.toString().equals("{\"dc2\":22}"))
	          	 	i++;
	          	else if(pos==5 && collection.toString().equals("{\"dc2\":22}"))
	          	 	i++;
	          	else if(pos==6 && collection.toString().equals("{\"dc2\":22}"))
	          	 	i++;
	          	else if(pos==7 && collection.toString().equals("{\"dc2\":22}"))
	          	 	i++;
	          	else if(pos==8 && collection.toString().equals("{\"dc2\":22}"))
	          	 	i++;
	          	else if(pos==9 && collection.toString().equals("{\"dc2\":22}"))
	          	 	i++;
	          	pos++;
	        }
	    	
	    	
	    	 rs=stmt.executeQuery("select {sp2,oc2,nv2,dc2} from DbTestCollection order by sp2,oc2,nv2,dc2");
		    	pos=0;
		    	while(rs.next()) {
		          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
		          	// System.out.println(collection +" "+pos);
		          	if(pos==0 && collection.toString().equals("{\"sp2\":1,\"oc2\":2,\"nv2\":2,\"dc2\":2}"))
		          	 	i++;
		          	else if(pos==1 && collection.toString().equals("{\"sp2\":4,\"oc2\":6,\"nv2\":2,\"dc2\":22}"))
		          	 	i++;
		          	else if(pos==2 && collection.toString().equals("{\"sp2\":5,\"oc2\":17,\"nv2\":2,\"dc2\":22}"))
		          	 	i++;
		          	else if(pos==3 && collection.toString().equals("{\"sp2\":5,\"oc2\":37,\"nv2\":1,\"dc2\":22}"))
		          	 	i++;
		          	else if(pos==4 && collection.toString().equals("{\"sp2\":5,\"oc2\":37,\"nv2\":2,\"dc2\":22}"))
		          	 	i++;
		          	else if(pos==5 && collection.toString().equals("{\"sp2\":5,\"oc2\":37,\"nv2\":3,\"dc2\":22}"))
		          	 	i++;
		        	else if(pos==6 && collection.toString().equals("{\"sp2\":15,\"oc2\":7,\"nv2\":9,\"dc2\":22}"))
		          	 	i++;
		        	else if(pos==7 && collection.toString().equals("{\"sp2\":15,\"oc2\":87,\"nv2\":7,\"dc2\":1}"))
		          	 	i++;
		        	else if(pos==8 && collection.toString().equals("{\"sp2\":15,\"oc2\":87,\"nv2\":7,\"dc2\":2}"))
		          	 	i++;
		        	else if(pos==9 && collection.toString().equals("{\"sp2\":15,\"oc2\":87,\"nv2\":7,\"dc2\":7}"))
		          	 	i++;
//		
		          	pos++;
		        }
		    	
	    	
	    	
		}
		finally{
			status=stmt.execute("drop collection DbTestCollection");
			status=stmt.execute("drop db DbTest");
		}
		con.close();
		Assert.assertEquals(20,i);
	}
	
	
	@Test
	public void aggrigationTest() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		//System.out.println(status);
		status=stmt.execute("create collection DbTestCollection");
		//System.out.println(status);

		PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"month\":3,\"year\":2018,\"dlrShipVersionId\":0,\"dealershipId\":0,\"dealerGroupId\":0,\"typeClassId\":4,\"baumusterId\":0,\"isActive\":\"Y\",\"countryId\":1,\"allocationRunId\":181,\"fieldName\":\"Manual Allocation Pool\",\"fieldId\":48,\"fieldCode\":\"REMAINING\",\"dc0\":0.0,\"t0\":0.0,\"ja\":0.0,\"fe\":0.0,\"mr\":0.0,\"ap\":0.0,\"my\":0.0,\"jn\":0.0,\"jl\":0.0,\"ag\":0.0,\"sp\":0.0,\"oc\":0.0,\"nv\":0.0,\"dc\":0.0,\"ja2\":0.0,\"fe2\":0.0,\"mr2\":0.0,\"ap2\":0.0,\"my2\":0.0,\"jn2\":0.0,\"jl2\":0.0,\"ag2\":0.0,\"sp2\":0.0,\"oc2\":0.0,\"nv2\":?,\"dc2\":?}");
		
		pstmt.setInt(2,1);
		pstmt.setInt(3,2);
		pstmt.setInt(1,696356);
		
    	status=pstmt.execute();
	
    	
    	pstmt.setInt(2,4);
		pstmt.setInt(3,6);
		pstmt.setInt(1,6);
		
    	status=pstmt.execute();
	
    	
    	pstmt.setInt(2,5);
		pstmt.setInt(3,7);
		pstmt.setInt(1,6);
		
    	status=pstmt.execute();
	
    	
    	ResultSet  rs=stmt.executeQuery("select {sum(dc2)} from DbTestCollection");
    	int i=0;
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
         //	System.out.println(collection);
          	 if(collection.toString().equals("{\"sum\":15}"))
          	 	i++;
        }
    	
    	rs=stmt.executeQuery("select count(*} from DbTestCollection order by dc2");
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
         	//System.out.println(collection);
          	 if(collection.toString().equals("{\"count\":3}"))
          	 	i++;
        }
    	
    	
    	rs=stmt.executeQuery("select {avg(dc2)} from DbTestCollection");
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
     //    	System.out.println(collection);
          	 if(collection.toString().equals("{\"avg\":5}"))
          	 	i++;
        }
    	
    	rs=stmt.executeQuery("select {min(dc2)} from DbTestCollection");
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
         	//System.out.println(collection);
          	 if(collection.toString().equals("{\"min\":2}"))
          	 	i++;
        }
    	
    	rs=stmt.executeQuery("select {max(dc2)} from DbTestCollection");
    	while(rs.next()) {
          	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
         	//System.out.println(collection);
          	 if(collection.toString().equals("{\"max\":7}"))
          	 	i++;
        }
    	
		status=stmt.execute("drop collection DbTestCollection");
		status=stmt.execute("drop db DbTest");

		con.close();
		Assert.assertEquals(5,i);
	}
	
	
	
	@Test
	public void transactionInsertRollback() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
	
		con.setAutoCommit(false);
        PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}]}");
		
		//System.out.println("start1 "+(new Date()));
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,2);
		pstmt.setInt(6,3);
    	status=pstmt.execute();
	
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	
    	
    	Statement stmnt=con.createStatement();
       	ResultSet  rs=stmnt.executeQuery("select * from DbTestCollection"); 
       	int i=0;

   	    while(rs.next()) {
         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
//             System.out.println("RS 1"+collection);
         	 i++;

        }
   	    
   	    Connection con1=createDnUnSQlConnection();
   	    Statement stmnt1=con1.createStatement();
   	    status=stmnt1.execute("use DbTest");
   	    
    	ResultSet  rs1=stmnt1.executeQuery("select * from DbTestCollection"); 
    	

	    while(rs1.next()) {
	      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs1.getObject(1);
	    //      System.out.println("RS 11"+collection);
	      	 i++;
	
	     }
   	    
     	con1.close(); 
       	con.rollback();
       	
        rs=stmnt.executeQuery("select * from DbTestCollection"); 
  
   	    while(rs.next()) {
         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
         //    System.out.println("RS 2"+collection);
         	 i++;

        }
       	
    	status=stmt.execute("drop db DbTest");
		con.close();
		Assert.assertEquals(2,i);
	}
	
	
	@Test
	public void transactionInsertCommit() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
	
		con.setAutoCommit(false);
        PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}]}");
		
		//System.out.println("start1 "+(new Date()));
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,2);
		pstmt.setInt(6,3);
    	status=pstmt.execute();
	
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	
    	
    	Statement stmnt=con.createStatement();
       	ResultSet  rs=stmnt.executeQuery("select * from DbTestCollection"); 
       	int i=0;

   	    while(rs.next()) {
         	// ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
        //     System.out.println("CM 1"+collection);
         	 i++;

        }
   	    
   	    Connection con1=createDnUnSQlConnection();
   	    Statement stmnt1=con1.createStatement();
   	    status=stmnt1.execute("use DbTest");
   	    
    	ResultSet  rs1=stmnt1.executeQuery("select * from DbTestCollection"); 
    	

	    while(rs1.next()) {
	      //	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs1.getObject(1);
	  //        System.out.println("CM 11"+collection);
	      	 i++;
	
	     }
   	    
     	con1.close(); 
       	con.commit();
       	
        rs=stmnt.executeQuery("select * from DbTestCollection"); 
  
   	    while(rs.next()) {
         	// ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
    //         System.out.println("CM 2"+collection);
         	 i++;

        }
       	
    	status=stmt.execute("drop db DbTest");
		con.close();
		Assert.assertEquals(4,i);
	}
		
	@Test
	public void transactionDeleteCommit() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
	
		
        PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}]}");
		
		//System.out.println("start1 "+(new Date()));
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,2);
		pstmt.setInt(6,3);
    	status=pstmt.execute();
	
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	pstmt.setInt(1,696358);
    	pstmt.setString(2,"\"Mita\"");
		pstmt.setString(3,"\"Mita123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	
    	
    	Statement stmnt=con.createStatement();
       	ResultSet  rs=stmnt.executeQuery("select * from DbTestCollection"); 
       	int i=0;

   	    while(rs.next()) {
         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
             //System.out.println("CM 1"+collection);
         	 i++;

        }
   	    
   	    con.setAutoCommit(false);
   	    stmnt.execute("delete from DbTestCollection where id=696357"); 
   	 
   	    Connection con1=createDnUnSQlConnection();
   	    Statement stmnt1=con1.createStatement();
   	    status=stmnt1.execute("use DbTest");
   	    int j=0;
    	ResultSet  rs1=stmnt1.executeQuery("select * from DbTestCollection"); 
    	

	    while(rs1.next()) {
	      	 //ImmenseDbCollection collection=(ImmenseDbCollection)  rs1.getObject(1);
	         // System.out.println("CM 11"+collection);
	      	 j++;
	
	     }
   	    
     
       
       	
        rs=stmnt.executeQuery("select * from DbTestCollection"); 
  
   	    while(rs.next()) {
         	// ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
            // System.out.println("CM 2"+collection);
         	 i++;

        }
       	
   	    
		con.commit();
			
		rs=stmnt.executeQuery("select * from DbTestCollection"); 
		
		while(rs.next()) {
		 //ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
		 // System.out.println("CM 3"+collection);
		  	 i++;
		
		 }
		
       rs1=stmnt1.executeQuery("select * from DbTestCollection"); 
    	

	    while(rs1.next()) {
	      	// ImmenseDbCollection collection=(ImmenseDbCollection)  rs1.getObject(1);
	          //System.out.println("CM 22"+collection);
	      	 j++;
	
	     }
		
		
    	status=stmt.execute("drop db DbTest");
    	
    	con1.close(); 
		con.close();
		Assert.assertEquals(7,i);
		Assert.assertEquals(5,j);
	}
		

	@Test
	public void transactionDeleteRollback() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
	
		
        PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}]}");
		
		//System.out.println("start1 "+(new Date()));
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,2);
		pstmt.setInt(6,3);
    	status=pstmt.execute();
	
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	pstmt.setInt(1,696358);
    	pstmt.setString(2,"\"Mita\"");
		pstmt.setString(3,"\"Mita123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	
    	
    	Statement stmnt=con.createStatement();
       	ResultSet  rs=stmnt.executeQuery("select * from DbTestCollection"); 
       	int i=0;

   	    while(rs.next()) {
//         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
//             System.out.println("CM 1"+collection);
         	 i++;

        }
   	    
   	    con.setAutoCommit(false);
   	    stmnt.execute("delete from DbTestCollection where id=696357"); 
   	 
   	    Connection con1=createDnUnSQlConnection();
   	    Statement stmnt1=con1.createStatement();
   	    status=stmnt1.execute("use DbTest");
   	    int j=0;
    	ResultSet  rs1=stmnt1.executeQuery("select * from DbTestCollection"); 
    	

	    while(rs1.next()) {
//	      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs1.getObject(1);
//	          System.out.println("CM 11"+collection);
	      	 j++;
	
	     }
   	    
     
       
       	
        rs=stmnt.executeQuery("select * from DbTestCollection"); 
  
   	    while(rs.next()) {
//         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
//             System.out.println("CM 2"+collection);
         	 i++;

        }
       	
   	    
		con.rollback();
			
		rs=stmnt.executeQuery("select * from DbTestCollection"); 
		
		while(rs.next()) {
//		 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
//		  System.out.println("CM 3"+collection);
		  	 i++;
		
		 }
		
       rs1=stmnt1.executeQuery("select * from DbTestCollection"); 
    	

	    while(rs1.next()) {
//	      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs1.getObject(1);
//	          System.out.println("CM 22"+collection);
	      	 j++;
	
	     }
		
		
    	status=stmt.execute("drop db DbTest");
    	
    	con1.close(); 
		con.close();
		Assert.assertEquals(8,i);
		Assert.assertEquals(6,j);
	}
	
	@Test
	public void transactionUpdateCommit() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
	
		
        PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}]}");
		
		//System.out.println("start1 "+(new Date()));
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,2);
		pstmt.setInt(6,3);
    	status=pstmt.execute();
	
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	pstmt.setInt(1,696358);
    	pstmt.setString(2,"\"Mita\"");
		pstmt.setString(3,"\"Mita123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	
    	
    	Statement stmnt=con.createStatement();
       	ResultSet  rs=stmnt.executeQuery("select * from DbTestCollection"); 
       	int i=0;

   	    while(rs.next()) {
         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
         	 
         	 if(collection.toString().indexOf("\"id\":696356")!=-1 || collection.toString().indexOf("\"id\":696357")!=-1 || collection.toString().indexOf("\"id\":696358")!=-1)
         		 i++;
       //      System.out.println("CM 1"+collection);
         	

        }
   	    
   	    con.setAutoCommit(false);
   	    stmnt.execute("update DbTestCollection set id=696359 where id=696357"); 
   	 
   	    Connection con1=createDnUnSQlConnection();
   	    Statement stmnt1=con1.createStatement();
   	    status=stmnt1.execute("use DbTest");
   	    int j=0;
    	ResultSet  rs1=stmnt1.executeQuery("select * from DbTestCollection"); 
    	

	    while(rs1.next()) {
	      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs1.getObject(1);
	        //  System.out.println("CM 11"+collection);
	      	if(collection.toString().indexOf("\"id\":696356")!=-1 || collection.toString().indexOf("\"id\":696357")!=-1 || collection.toString().indexOf("\"id\":696358")!=-1)
        		
	      	 j++;
	
	     }
   	    
     
       
       	
        rs=stmnt.executeQuery("select * from DbTestCollection"); 
  
   	    while(rs.next()) {
         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
            // System.out.println("CM 2"+collection);
            if(collection.toString().indexOf("\"id\":696356")!=-1 || collection.toString().indexOf("\"id\":696359")!=-1 || collection.toString().indexOf("\"id\":696358")!=-1)
             i++;

        }
       	
   	    
		con.commit();
			
		rs=stmnt.executeQuery("select * from DbTestCollection"); 
		
		while(rs.next()) {
		 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
		//  System.out.println("CM 3"+collection);
         if(collection.toString().indexOf("\"id\":696356")!=-1 || collection.toString().indexOf("\"id\":696359")!=-1 || collection.toString().indexOf("\"id\":696358")!=-1)
		  	 i++;
		
		 }
		
       rs1=stmnt1.executeQuery("select * from DbTestCollection"); 
    	

	    while(rs1.next()) {
	      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs1.getObject(1);
	    //      System.out.println("CM 22"+collection);
	        if(collection.toString().indexOf("\"id\":696356")!=-1 || collection.toString().indexOf("\"id\":696359")!=-1 || collection.toString().indexOf("\"id\":696358")!=-1)
	        		j++;
	
	     }
		
		
    	status=stmt.execute("drop db DbTest");
    	
    	con1.close(); 
		con.close();
		Assert.assertEquals(9,i);
		Assert.assertEquals(6,j);
	}
		
	@Test
	public void transactionUpdateRollback() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
	
		
        PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}]}");
		
		//System.out.println("start1 "+(new Date()));
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,2);
		pstmt.setInt(6,3);
    	status=pstmt.execute();
	
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	pstmt.setInt(1,696358);
    	pstmt.setString(2,"\"Mita\"");
		pstmt.setString(3,"\"Mita123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	
    	
    	Statement stmnt=con.createStatement();
       	ResultSet  rs=stmnt.executeQuery("select * from DbTestCollection"); 
       	int i=0;

   	    while(rs.next()) {
         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
             //System.out.println("CM 1"+collection);
             if(collection.toString().indexOf("\"id\":696356")!=-1 || collection.toString().indexOf("\"id\":696357")!=-1 || collection.toString().indexOf("\"id\":696358")!=-1)

         	 i++;

        }
   	    
   	    con.setAutoCommit(false);
   	    stmnt.execute("update DbTestCollection set id=696359 where id=696357"); 
   	 
   	    Connection con1=createDnUnSQlConnection();
   	    Statement stmnt1=con1.createStatement();
   	    status=stmnt1.execute("use DbTest");
   	    int j=0;
    	ResultSet  rs1=stmnt1.executeQuery("select * from DbTestCollection"); 
    	

	    while(rs1.next()) {
	      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs1.getObject(1);
	         // System.out.println("CM 11"+collection);
	         if(collection.toString().indexOf("\"id\":696356")!=-1 || collection.toString().indexOf("\"id\":696357")!=-1 || collection.toString().indexOf("\"id\":696358")!=-1)
        	 	j++;
	
	     }
   	    
     
       
       	
        rs=stmnt.executeQuery("select * from DbTestCollection"); 
  
   	    while(rs.next()) {
         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
           //  System.out.println("CM 2"+collection);
             if(collection.toString().indexOf("\"id\":696356")!=-1 || collection.toString().indexOf("\"id\":696359")!=-1 || collection.toString().indexOf("\"id\":696358")!=-1)
            	 i++;

        }
       	
   	    
		con.rollback();
			
		rs=stmnt.executeQuery("select * from DbTestCollection"); 
		
		while(rs.next()) {
		 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
		 // System.out.println("CM 3"+collection);
          if(collection.toString().indexOf("\"id\":696356")!=-1 || collection.toString().indexOf("\"id\":696357")!=-1 || collection.toString().indexOf("\"id\":696358")!=-1)
        	  i++;
		
		 }
		
       rs1=stmnt1.executeQuery("select * from DbTestCollection"); 
    	

	    while(rs1.next()) {
	      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs1.getObject(1);
	         //ystem.out.println("CM 22"+collection);
            if(collection.toString().indexOf("\"id\":696356")!=-1 || collection.toString().indexOf("\"id\":696357")!=-1 || collection.toString().indexOf("\"id\":696358")!=-1)
	      	 j++;
	
	     }
		
		
    	status=stmt.execute("drop db DbTest");
    	
    	con1.close(); 
		con.close();
		Assert.assertEquals(9,i);
		Assert.assertEquals(6,j);
	}
		
	
	
	
	@Test
	public void transactionLock() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		status=stmt.execute("create collection DbTestCollection");
	
		
        PreparedStatement pstmt=con.prepareStatement("insert into DbTestCollection value {\"id\":?,\"userId\":?,\"password\":?,\"access\":[{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"Y\"},{\"accessId\":?,\"isActive\":\"N\"}]}");
		
		//System.out.println("start1 "+(new Date()));
		pstmt.setInt(1,696356);
		pstmt.setString(2,"\"dibakar\"");
		pstmt.setString(3,"\"dibakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,2);
		pstmt.setInt(6,3);
    	status=pstmt.execute();
	
    	pstmt.setInt(1,696357);
    	pstmt.setString(2,"\"pravakar\"");
		pstmt.setString(3,"\"pravakar123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	pstmt.setInt(1,696358);
    	pstmt.setString(2,"\"Mita\"");
		pstmt.setString(3,"\"Mita123\"");
		pstmt.setInt(4,1);
		pstmt.setInt(5,6);
		pstmt.setInt(6,5);
    	status=pstmt.execute();
		
    	
    	
    	Statement stmnt=con.createStatement();
       	ResultSet  rs=stmnt.executeQuery("select * from DbTestCollection"); 
       	int i=0;

   	    while(rs.next()) {
         	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
            // System.out.println("CM 1"+collection);
         	 i++;

        }
   	    
   	    con.setAutoCommit(false);
   	    stmnt.execute("delete from DbTestCollection where id=696357"); 
   	 
   	    Connection con1=createDnUnSQlConnection();
   	    Statement stmnt1=con1.createStatement();
   	    status=stmnt1.execute("use DbTest");
   	    int j=0;
    	
   	    try{
   	    	stmnt1.executeQuery("update DbTestCollection set userId='transaction' where id=696357"); 
   	    }catch(Exception e){
   	    //	System.out.println("Error "+ e.getMessage());
   	    	if(e.getMessage().equals("{\"status\":\"failed\",\"count\":\"0\",\"message\":\"Transaction timeout. Resource is locked\"}"))
   	    		j++;
   	    }
		con.commit();
			
		rs=stmnt.executeQuery("update DbTestCollection set userId='transaction' where id=696357"); 
   	    
		
		
    	status=stmt.execute("drop db DbTest");
    	
    	con1.close(); 
		con.close();
		Assert.assertEquals(3,i);
		Assert.assertEquals(1,j);
	}
		
	
	
	
	@Test
	public void indexAddRemoveAlter() throws Exception {
	con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status;
	
		status=stmt.execute("create db DbTest");
		status=stmt.execute("use DbTest");
		//status=stmt.execute("drop index dnTestIII");
		status=stmt.execute("create index dnTestIII on testCollection");

    	Statement stmnt=con.createStatement();
       	int i=0;
        
        ResultSet  rs=stmnt.executeQuery("show indexes");// where address.cityCode=5"); 
	    while(rs.next()) {
      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
      	 if(collection.toString().equals("{\"name\":\"dnTestIII\"}"))      		 
      		 i++;
          //System.out.println("CM 1"+collection);
        }
    	
	   status=stmt.execute("alter index dnTestIII dnTestA");
	    rs=stmnt.executeQuery("show indexes");// where address.cityCode=5"); 
	    while(rs.next()) {
      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
      	 if(collection.toString().equals("{\"name\":\"dnTestA\"}"))      		 
      		 i++;

      		// System.out.println("CM 2"+collection);
        }
////	    
	    try{
	    	status=stmt.execute("drop index dnTestA");
	    }
	    catch(Exception e){
	    	
	    }
	    
	    rs=stmnt.executeQuery("show indexes");// where address.cityCode=5"); 
	    while(rs.next()) {
      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
      //	 if(collection.toString().equals("{\"user\":\"manager\",\"password\":\"pwd123\",\"role\":[]}"))      		 
      		 i++;
          System.out.println("CM 3"+collection);
        }
	   
	    status=stmt.execute("drop db DbTest");
    	con.close();
		Assert.assertEquals(2,i);

	}	
	
	
	@Test
	public void closeDb() throws Exception {
		con=createDnUnSQlConnection();
		con.close();
		Assert.assertEquals(true, con.isClosed());
	}
	
	//@Test
	public void createNConnection() throws Exception {
		int i=0;
		while(++i<500){
			con=createDnUnSQlConnection();
			con.close();
		}
		Assert.assertEquals(500,i);
		
	}
	
	@Test
	public void userAddRemoveAlter() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status;
		//stmt.execute("drop db DbTest");
		//status=stmt.execute("create db DbTest");
		status=stmt.execute("create user manager identified by pwd123");
	 	
    	Statement stmnt=con.createStatement();
       	int i=0;
        
        ResultSet  rs=stmnt.executeQuery("show users");// where address.cityCode=5"); 
	    while(rs.next()) {
      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
      	 if(collection.toString().equals("{\"user\":\"manager\",\"password\":\"pwd123\",\"role\":[]}"))      		 
      		 i++;
          //System.out.println("CM 1"+collection);
        }
    	
	    status=stmt.execute("alter user manager identified by pwd2345");
	    rs=stmnt.executeQuery("show users");// where address.cityCode=5"); 
	    while(rs.next()) {
      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
      	 if(collection.toString().equals("{\"user\":\"manager\",\"password\":\"pwd2345\",\"role\":[]}"))      		 
      		 i++;
         // System.out.println("CM 1"+collection);
        }
	    
	    status=stmt.execute("drop user manager");
	    rs=stmnt.executeQuery("show users");// where address.cityCode=5"); 
	    while(rs.next()) {
      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
      	 if(collection.toString().equals("{\"user\":\"manager\",\"password\":\"pwd123\",\"role\":[]}"))      		 
      		 i++;
          //System.out.println("CM 1"+collection);
        }
	    
    	con.close();
		Assert.assertEquals(2,i);

	}	
	
	@Test
	public void roleGrantRevoke() throws Exception {
		con=createDnUnSQlConnection();
		
		Statement stmt=con.createStatement();
		boolean status;
		//stmt.execute("drop db DbTest");
		//status=stmt.execute("drop user manager");
		status=stmt.execute("create user manager identified by pwd123");
	 	
    	Statement stmnt=con.createStatement();
       	int i=0;
        
        ResultSet  rs=stmnt.executeQuery("show users");// where address.cityCode=5"); 
	    while(rs.next()) {
      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
      	 if(collection.toString().equals("{\"user\":\"manager\",\"password\":\"pwd123\",\"role\":[]}"))      		 
      		 i++;
        ///  System.out.println("CM 1"+collection);
        }
    	
	    status=stmt.execute("grant insert on testdb to manager");
	    rs=stmnt.executeQuery("show users");// where address.cityCode=5"); 
	    while(rs.next()) {
      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
      	 if(collection.toString().equals("{\"user\":\"manager\",\"password\":\"pwd123\",\"role\":[\"insert on testdb\"]}"))      		 
      		 i++;
         /// System.out.println("CM 2"+collection);
        }
	    
	    status=stmt.execute("grant admin to manager");
	    rs=stmnt.executeQuery("show users");// where address.cityCode=5"); 
	    while(rs.next()) {
      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
      	 if(collection.toString().equals("{\"user\":\"manager\",\"password\":\"pwd123\",\"role\":[\"admin\",\"insert on testdb\"]}"))      		 
      		 i++;
         // System.out.println("CM 3"+collection);
        }
	    
	    
	    status=stmt.execute("revoke admin from manager");
	    rs=stmnt.executeQuery("show users");// where address.cityCode=5"); 
	    while(rs.next()) {
      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
      	 if(collection.toString().equals("{\"user\":\"manager\",\"password\":\"pwd123\",\"role\":[\"insert on testdb\"]}"))      		 
      		 i++;
         //System.out.println("CM 4"+collection);
        }
	    
	    
	    status=stmt.execute("revoke insert on testdb from manager");
	    rs=stmnt.executeQuery("show users");// where address.cityCode=5"); 
	    while(rs.next()) {
      	 ImmenseDbCollection collection=(ImmenseDbCollection)  rs.getObject(1);
      	 if(collection.toString().equals("{\"user\":\"manager\",\"password\":\"pwd123\",\"role\":[]}"))      		 
      		 i++;
         // System.out.println("CM 5"+collection);
        }
	    
	    status=stmt.execute("drop user manager");
	    
    	con.close();
		Assert.assertEquals(5,i);

	}	

	
	public Connection createDnUnSQlConnection() {
        Connection con = null;
      
                try {
                	    Class.forName("com.immensedb.jdbc.Driver");
                        con = DriverManager.getConnection(
                        "jdbc:immensedb://localhost:3107/testdb", "admin","admin");
                                      }
                 catch (Exception e) {
                        System.out.println("Connection Error: "+e.toString());
                        e.printStackTrace();
                }
      
        return con;
    }

}
