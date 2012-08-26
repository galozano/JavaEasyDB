package easyDB.mundo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * 
 * @author gustavolozano
 *
 */
public class EasyDB
{
	//--------------------------------------------------------------
	// Attributes
	//--------------------------------------------------------------
	
	private StringBuilder query;
	
	private ArrayList<String> values;

	private static Connection conn;

	private String database;
	
	//--------------------------------------------------------------
	// Constructors
	//--------------------------------------------------------------
	
	public EasyDB(String host, String database,String username, String password) throws ClassNotFoundException, SQLException
	{
		this.query = new StringBuilder();	
		this.values = new ArrayList<String>();
		
		this.database = database;
		host = host + database;
		
		connect(host, username, password);
	}
	
	//--------------------------------------------------------------
	// Private Methods
	//--------------------------------------------------------------

	/**
	 * 
	 * @param host: ej. jdbc:mysql://localhost:3306/kahuu"
	 * @param username
	 * @param password
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private void connect(String host, String username, String password) throws ClassNotFoundException, SQLException
	{
		Class.forName( "com.mysql.jdbc.Driver" );
		conn = DriverManager.getConnection(host, username, password );
	}

	//--------------------------------------------------------------
	// Methods
	//--------------------------------------------------------------
	
	public void Select(String columns)
	{
		this.query.append("SELECT ");
		this.query.append(columns);
		this.query.append(" ");
	}

	public void From(String table)
	{
		this.query.append("FROM ");
		this.query.append(this.database+ "." +table);
		this.query.append(" ");
	}

	public void Where(String[] where)
	{
		this.query.append("WHERE ");

		for (int i = 0; i < where.length ; i++) 
		{
			query.append(where[i]);

			if(where.length != (i + 1))
			{
				query.append(" AND ");
			}
		}
	}
	
	public void Order(String order)
	{
		this.query.append("ORDER BY ");
		this.query.append(order);
	}

	public void GroupBy(String group)
	{
		this.query.append("GROUP BY ");
		this.query.append(group);
	}

	public void InsertValues(String table, String[] values)
	{
		this.query.append("INSERT INTO ");
		this.query.append(this.database+ "." +table);
		
		StringBuilder sql1 = new StringBuilder();
		StringBuilder sql2 = new StringBuilder();

		sql1.append(" ( ");
		sql2.append(" ( ");
		
		for (int i = 0; i < values.length; i++) 
		{
			String[] value = values[i].split("=");
			
			sql1.append(value[0]);
			
			if(i < values.length - 1)
				sql1.append(",");
			
			sql2.append(value[1]);
			
			if(i < values.length - 1)
				sql2.append(",");
		}
		
		sql1.append(" ) ");
		sql2.append(" ) ");
		
		this.query.append(sql1);
		this.query.append("VALUES ");
		this.query.append(sql2);
	}
	
	public void Insert(String table)
	{
		this.query.append("INSERT INTO ");
		this.query.append(this.database+ "." +table);
		
		StringBuilder sql1 = new StringBuilder();
		StringBuilder sql2 = new StringBuilder();

		sql1.append(" ( ");
		sql2.append(" ( ");
		
		for (int i = 0; i < this.values.size(); i++) 
		{
			String[] value = values.get(i).split("=");
			
			sql1.append(value[0]);
			
			if(i < this.values.size()-1)
				sql1.append(",");
			
			sql2.append(value[1]);
			
			if(i < this.values.size()-1)
				sql2.append(",");
		}
		
		sql1.append(" ) ");
		sql2.append(" ) ");
		
		this.query.append(sql1);
		this.query.append("VALUES ");
		this.query.append(sql2);
	}
	
	public void UpdateValues(String table, String[] values, String[] where)
	{
		this.query.append("UPDATE ");
		this.query.append(this.database + "." + table);
		
		StringBuilder sql1 = new StringBuilder();
		sql1.append(" SET ");
		
		for (int i = 0; i < values.length; i++) 
		{	
			sql1.append(values[0]);
			
			if(i < values.length - 1)
				sql1.append(",");
		}
		
		sql1.append(" ");
		this.query.append(sql1);
		this.Where(where);
	}
	
	public void Update(String table, String[] where)
	{
		this.query.append("UPDATE ");
		this.query.append(table);
		
		StringBuilder sql1 = new StringBuilder();
		StringBuilder sql2 = new StringBuilder();

		sql1.append("( ");
		sql2.append("( ");
		
		for (int i = 0; i < this.values.size(); i++) 
		{
			String[] value = values.get(i).split("=");
			
			sql1.append(value[0]);
			
			if(i < this.values.size()-1)
				sql1.append(",");
			
			sql2.append(value[1]);
			
			if(i < this.values.size()-1)
				sql2.append(",");
		}
		
		sql1.append(") ");
		sql2.append(") ");
		
		this.query.append(sql1);
		this.query.append("SET ");
		this.query.append(sql2);
	}

	public void DeleteTable(String table)
	{
		this.query.append("DROP TABLE ");
		this.query.append(table);
	}
	
	public void DeleteRowValues(String table, String[] values)
	{
		this.query.append("DELETE FROM ");
		this.query.append(table);
		
		this.Where(values);
	}
	
	public void InnerJoin(String table, String on)
	{
		//INNER JOIN	
		this.query.append("INNER JOIN ");
		this.query.append(this.database + "." +table);
		
		this.query.append(" ON ");
		this.query.append(on);
	}

	public void addString(String column, String value)
	{
		String add = column + "=\"" + value + "\"";
		values.add(add);
	}
	
	public void addInt(String column, int value)
	{
		String add = column + "=" + value;
		values.add(add);
	}
	
	public void addLong(String column, long value)
	{
		String add = column + "=" + value;
		values.add(add);
	}
	
	public void execute( ) throws SQLException
	{
		PreparedStatement stmt = conn.prepareStatement(query.toString());		
		stmt.execute();

		this.query = null;
		this.query = new StringBuilder();
	}
	
	public ResultSet executeQuery( ) throws SQLException
	{		
		PreparedStatement stmt = conn.prepareStatement(query.toString());		
		ResultSet result =  stmt.executeQuery();

		this.query = new StringBuilder();

		return result;
	}

	public ResultSet executeSQL(String sql) throws SQLException
	{
		PreparedStatement stmt = conn.prepareStatement(sql);	
		ResultSet result =  stmt.executeQuery();

		return result;
	}
	
	public void closeConnection( ) throws SQLException
	{
		conn.close( );
	}

	public void begginTransaccion( ) throws SQLException
	{
		conn.setAutoCommit(false);
	}

	public void commit( ) throws SQLException
	{
		conn.commit();
	}
	
	public void rollback( ) throws SQLException
	{
		conn.rollback();
	} 
	
	public Connection getConnection( )
	{
		return conn;
	}
	
	public String getQuery( )
	{
		return query.toString();
	}
}
