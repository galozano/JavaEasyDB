/**
 * 
 */
package easyDB.test;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import easyDB.mundo.EasyDB;

/**
 * @author gustavolozano
 *
 */
public class EasyDBTest 
{

	//----------------------------------------------------------------------
	// Constants
	//----------------------------------------------------------------------

	public static final String URL = "jdbc:mysql://kelgal.com:3306/prueba";

	public static final String USER = "afuera";

	public static final String PASSWORD = "qazxsw23edc";

	//----------------------------------------------------------------------
	// Aributtes
	//----------------------------------------------------------------------

	private static EasyDB easy;

	private static Connection conn;

	//----------------------------------------------------------------------
	// Private Methods
	//----------------------------------------------------------------------

	private void connect(String host, String username, String password) throws ClassNotFoundException, SQLException
	{
		Class.forName( "com.mysql.jdbc.Driver" );
		conn = DriverManager.getConnection(host, username, password );
	}

	//----------------------------------------------------------------------
	// Configure Methods
	//----------------------------------------------------------------------

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception 
	{
		easy = new EasyDB("jdbc:mysql://kelgal.com:3306/", "prueba","afuera","qazxsw23edc");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
		easy.closeConnection();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception
	{
		connect(URL, USER, PASSWORD);

		String sql1 = "CREATE TABLE Users (id INT NOT NULL, PRIMARY KEY(id), name VARCHAR(30), age INT)";
		String sql2 = "CREATE TABLE Items (id BIGINT NOT NULL, PRIMARY KEY(id), name VARCHAR(45), price DOUBLE, idUser INT, FOREIGN KEY (idUser) REFERENCES USERS(id))";
		
		//Insert information in tables
		String sql3 = "INSERT INTO Users (id,name, age) VALUES (1,\"Gus\", 22)";
		String sql4 = "INSERT INTO Items (id,name,price,idUser) VALUES (1,\"Comp\",25.0,1)";

		PreparedStatement stmt = conn.prepareStatement(sql1);
		PreparedStatement stmt2 = conn.prepareStatement(sql2);
		PreparedStatement stmt3 = conn.prepareStatement(sql3);
		PreparedStatement stmt4 = conn.prepareStatement(sql4);
		
		stmt.execute();
		stmt2.execute();
		stmt3.execute();	
		stmt4.execute();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown()
	{
		String sql1 = "DROP TABLE Users";	
		String sql2 = "DROP TABLE Items";

		try
		{
			PreparedStatement stmt1 = conn.prepareStatement(sql1);
			stmt1.execute();	
		} 
		catch (SQLException e) 
		{
			// The delete test needs to delete the user table
		}
		
		try 
		{
			PreparedStatement stmt2 = conn.prepareStatement(sql2);
			stmt2.execute();
		} 
		catch (SQLException e)
		{
			fail(e.getMessage());
		}	
	}

	//----------------------------------------------------------------------
	// Tests Methods
	//----------------------------------------------------------------------

	@Test
	public void testSelect() 
	{	
		try 
		{
			easy.Select("name");
			easy.From("Users");

			ResultSet result = easy.executeQuery();

			if(result.next())
			{
				assertEquals(result.getString("name"), "Gus");				
			}
			else
			{
				fail("There is no rows");
			}
		} 
		catch (SQLException e) 
		{
			fail(e.getMessage());
		}	
	}

	@Test
	public void testInsertValues( )
	{
		try
		{		
			String[] values = {"id=2","name=\"gustavo\"", "age=22"};		
			easy.InsertValues("Users", values);		

			System.out.println("QUERY EXECUTED: " + easy.getQuery());

			easy.execute();

			String[] where = {"name=\"gustavo\""};	
			easy.Select("name");
			easy.From("Users");
			easy.Where(where);

			ResultSet result = easy.executeQuery();

			if(result.next())
			{
				assertEquals(result.getString("name"), "gustavo");				
			}
			else
			{
				fail("There is no rows");
			}	
		}
		catch (SQLException e) 
		{
			fail(e.getMessage());
		}
	}

	@Test
	public void testIntert( )
	{
		try
		{		
			easy.addInt("id", 3);
			easy.addString("name", "cami");
			easy.addInt("age", 22);
			easy.Insert("Users");		

			System.out.println("QUERY EXECUTED: " + easy.getQuery());

			easy.execute();

			String[] where = {"name=\"cami\""};	
			easy.Select("name");
			easy.From("Users");
			easy.Where(where);

			ResultSet result = easy.executeQuery();

			if(result.next())
			{
				assertEquals(result.getString("name"), "cami");				
			}
			else
			{
				fail("There is no rows");
			}	
		}
		catch (SQLException e) 
		{
			fail(e.getMessage());
		}
	}

	@Test
	public void testUpdateValues( )
	{
		try
		{		
			String[] values = {"age=23"};	
			String[] where  = {"name=\"Gus\""};
			easy.UpdateValues("Users", values, where);	

			System.out.println("QUERY EXECUTED: " + easy.getQuery());

			easy.execute();

			String[] where2 = {"name=\"Gus\""};	
			easy.Select("age");
			easy.From("Users");
			easy.Where(where2);

			ResultSet result = easy.executeQuery();

			if(result.next())
			{
				assertEquals(result.getInt("age"), 23);				
			}
			else
			{
				fail("There is no rows");
			}			
		}
		catch (SQLException e) 
		{
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testJOIN( )
	{	
		try 
		{
			String on = "Users.id=Items.idUser";
			
			easy.Select("Users.id,Items.name");
			easy.From("Users");
			easy.InnerJoin("Items", on );
			
			System.out.println("QUERY EXECUTED: " + easy.getQuery());

			ResultSet result = easy.executeQuery();

			if(result.next())
			{
				assertEquals(result.getInt("id"), 1);
				assertEquals(result.getString("name"), "Comp");
			}
			else
			{
				fail("There is no rows");
			}		
		}
		catch (SQLException e) 
		{
			fail(e.getMessage());
		}	
	}
	
	@Test
	public void testDeleteRow( )
	{
		
	}

	@Test(expected=SQLException.class)
	public void testDeleteTable( ) throws SQLException
	{	
		easy.DeleteTable("Users");

		System.out.println("QUERY EXECUTED: " + easy.getQuery());

		easy.execute();

		easy.Select("age");
		easy.From("Users");

		ResultSet result = easy.executeQuery();

		if(result.next())
		{
			fail("The table is no supossed to exist");
		}
	}
}
