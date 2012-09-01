/**
   Copyright 2012 Gustavo Adolfo Lozano Velez

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

	JavaEasyDB is a light framework for small projects that want to connect 
	to a database in a simple and straight way. 
*/

package easyDB.mundo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


/**

   JavaEasyDB main class
   @author Gustavo Adolfo Lozano Velez
 */
public class EasyDB
{
	//--------------------------------------------------------------
	// Attributes
	//--------------------------------------------------------------
	
	private String select;
	
	private String from;
	
	private StringBuilder where;
	
	private String join;

	/**
	 * The values of the query
	 */
	private ArrayList<String> values;

	/**
	 * The connection to the database
	 */
	private static Connection conn;

	/**
	 * The database or schem name
	 */
	private String database;

	//--------------------------------------------------------------
	// Constructors
	//--------------------------------------------------------------

	/**
	 * Starting EasyDB using simple straight parameters
	 * @param host - database host
	 * @param database - database schema
	 * @param username - database username
	 * @param password - databse password
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public EasyDB(String host, String database,String username, String password) throws ClassNotFoundException, SQLException
	{		
		this.select = "";
		this.from = "";
		this.where = new StringBuilder();
		this.join= "";
					
		this.values = new ArrayList<String>();

		this.database = database;
		host = host + database;

		connect(host, username, password);
	}

	//--------------------------------------------------------------
	// Private Methods
	//--------------------------------------------------------------

	/**
	 * Method to connect to the database
	 * @param host: ej. jdbc:mysql://localhost:3306/kahuu"
	 * @param username database username
	 * @param password database password
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

	/**
	 * SQL SELECT
	 * @param columns columns to select 
	 */
	public void Select(String columns)
	{
		this.select = "SELECT " + columns + " ";
	}

	/**
	 * SQL FROM
	 * @param table tables or tables
	 */
	public void From(String table)
	{
		this.from = "FROM " + this.database+ "." +table + " ";
	}

	/**
	 * SQL WHERE
	 * @param where ej. {age=34,name=\"fred\"}
	 */
	public void WhereValues(String[] where)
	{
		this.where.append("WHERE ");

		for (int i = 0; i < where.length ; i++) 
		{
			this.where.append(where[i]);

			if(where.length != (i + 1))
			{
				this.where.append(" AND ");
			}
		}
	}
	
	/**
	 * 
	 * @param where
	 */
	public void Where(String where)
	{
		if(this.where.toString().equals(""))
		{
			this.where.append("WHERE ");
			this.where.append(where);
		}
		else
		{
			this.where.append(" AND ");
			this.where.append(where);
		}
	}
	
	
	

	/**
	 * SQL ORDER
	 * @param order
	 */
	public void Order(String order)
	{
//		this.query.append("ORDER BY ");
//		this.query.append(order);
	}

	/**
	 * SQL GROUP BY
	 * @param group
	 */
	public void GroupBy(String group)
	{
//		this.query.append("GROUP BY ");
//		this.query.append(group);
	}

	/**
	 * Comple Insert Statement
	 * @param table
	 * @param values 
	 * @throws SQLException 
	 */
	public void Insert(String table, String[] values) throws SQLException
	{
		StringBuilder query = new StringBuilder();
		
		query.append("INSERT INTO ");
		query.append(this.database+ "." +table);

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

		query.append(sql1);
		query.append("VALUES ");
		query.append(sql2);
		
		execute(query.toString());
	}

	/**
	 * 
	 * @param table
	 * @throws SQLException 
	 */
	public void InsertValues(String table) throws SQLException
	{
		StringBuilder query = new StringBuilder();

		query.append("INSERT INTO ");
		query.append(this.database+ "." +table);
		
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

		
		query.append(sql1);
		query.append(" VALUES ");
		query.append(sql2);
		
		execute(query.toString());
	}

	/**
	 * 
	 * @param table
	 * @param values
	 * @param where
	 * @throws SQLException 
	 */
	public void Update(String table, String[] values) throws SQLException
	{
		StringBuilder query = new StringBuilder();
		
		query.append("UPDATE ");
		query.append(this.database + "." + table);

		StringBuilder sql1 = new StringBuilder();
		sql1.append(" SET ");

		for (int i = 0; i < values.length; i++) 
		{	
			sql1.append(values[0]);

			if(i < values.length - 1)
				sql1.append(",");
		}

		sql1.append(" ");
		query.append(sql1);
		
		query.append(where.toString());
		
		execute(query.toString());
	}

	/**
	 * 
	 * @param table
	 * @param where
	 * @throws SQLException 
	 */
	public void UpdateValues(String table) throws SQLException
	{
		StringBuilder query = new StringBuilder();
		
		query.append("UPDATE ");
		query.append(table);

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

		query.append(sql1);
		query.append("SET ");
		query.append(sql2);
		
		execute(query.toString());
	}

	/**
	 * 
	 * @param table
	 * @throws SQLException 
	 */
	public void DeleteTable(String table) throws SQLException
	{
		StringBuilder query = new StringBuilder();
		
		query.append("DROP TABLE ");
		query.append(table);
		
		execute(query.toString());
	}

	/**
	 * 
	 * @param table
	 * @param values
	 * @throws SQLException 
	 */
	public void DeleteRow(String table) throws SQLException
	{
		StringBuilder query = new StringBuilder();
		
		query.append("DELETE FROM ");
		query.append(table);
		query.append(" ");
		
		query.append(this.where);
		execute(query.toString());
	}

	/**
	 * 
	 * @param table
	 * @param on
	 */
	public void InnerJoin(String table, String on)
	{
		//INNER JOIN	
		join = "INNER JOIN " + this.database + "." + table + " ON " + on;
	}

	/**
	 * 
	 * @param column
	 * @param value
	 */
	public void addString(String column, String value)
	{
		String add = column + "=\"" + value + "\"";
		values.add(add);
	}

	/**
	 * 
	 * @param column
	 * @param value
	 */
	public void addInt(String column, int value)
	{
		String add = column + "=" + value;
		values.add(add);
	}

	/**
	 * 
	 * @param column
	 * @param value
	 */
	public void addLong(String column, long value)
	{
		String add = column + "=" + value;
		values.add(add);
	}

	/**
	 * 
	 * @throws SQLException
	 */
	public void execute(String query) throws SQLException
	{
		System.out.println("QUERY EXECUTED: " + query);
		
		PreparedStatement stmt = conn.prepareStatement(query);		
		stmt.execute();
	
		reset();
	}
	
	private void reset( )
	{
		select = "";
		from = "";
		where = new StringBuilder();
		values = new ArrayList<String>();
		join = "";
	}

	/**
	 * 
	 * @return
	 * @throws SQLException
	 */
	public ResultSet executeQuery( ) throws SQLException
	{		
		
		
		String query = select + from + join + where;
		
		System.out.println("QUERY EXECUTED: " + query);
		
		PreparedStatement stmt = conn.prepareStatement(query.toString());		
		ResultSet result =  stmt.executeQuery();

		reset();

		return result;
	}
	
	/**
	 * Returns the string of the query being build
	 * @return
	 */
	public String getQuery( )
	{
		StringBuilder query = new StringBuilder();
		query.append(select);
		query.append(from);
		query.append(where);
		
		return query.toString();
	}

	/**
	 * 
	 * @param sql
	 * @param values
	 * @return
	 * @throws SQLException
	 */
	public ResultSet executeSQL(String sql, String[] values) throws SQLException
	{
		for(String value: values)
		{
			sql = sql.replaceFirst("\\?", value);		
		}
		
		System.out.println("SQL:" + sql);
		
		
		PreparedStatement stmt = conn.prepareStatement(sql);		
		ResultSet result =  stmt.executeQuery();
		
		return result;
	}

	/**
	 * 
	 * @throws SQLException
	 */
	public void closeConnection( ) throws SQLException
	{
		conn.close( );
	}

	/**
	 * 
	 * @throws SQLException
	 */
	public void begginTransaccion( ) throws SQLException
	{
		conn.setAutoCommit(false);
	}

	/**
	 * 
	 * @throws SQLException
	 */
	public void commit( ) throws SQLException
	{
		conn.commit();
	}

	/**
	 * 
	 * @throws SQLException
	 */
	public void rollback( ) throws SQLException
	{
		conn.rollback();
	} 

	/**
	 * Returns the current connection with the databse
	 * @return
	 */
	public Connection getConnection( )
	{
		return conn;
	}
}
