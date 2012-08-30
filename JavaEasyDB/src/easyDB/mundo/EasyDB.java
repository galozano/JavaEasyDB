package easyDB.mundo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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

   JavaEasyDB main class
 * @author Gustavo Adolfo Lozano Velez
 */
public class EasyDB
{
	//--------------------------------------------------------------
	// Attributes
	//--------------------------------------------------------------

	/**
	 * The query being build
	 */
	private StringBuilder query;

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
	 * @param host 
	 * @param database
	 * @param username
	 * @param password
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
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
	 * @param columns columns to select ej. name,age
	 */
	public void Select(String columns)
	{
		this.query.append("SELECT ");
		this.query.append(columns);
		this.query.append(" ");
	}

	/**
	 * SQL FROM
	 * @param table tables or tables ej. Users
	 */
	public void From(String table)
	{
		this.query.append("FROM ");
		this.query.append(this.database+ "." +table);
		this.query.append(" ");
	}

	/**
	 * SQL WHERE
	 * @param where ej. {age=34,name=\"fred\"}
	 */
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

	/**
	 * SQL ORDER
	 * @param order
	 */
	public void Order(String order)
	{
		this.query.append("ORDER BY ");
		this.query.append(order);
	}

	/**
	 * SQL GROUP BY
	 * @param group
	 */
	public void GroupBy(String group)
	{
		this.query.append("GROUP BY ");
		this.query.append(group);
	}

	/**
	 * Comple Insert Statement
	 * @param table
	 * @param values 
	 */
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

	/**
	 * 
	 * @param table
	 */
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

	/**
	 * 
	 * @param table
	 * @param values
	 * @param where
	 */
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

	/**
	 * 
	 * @param table
	 * @param where
	 */
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

	/**
	 * 
	 * @param table
	 */
	public void DeleteTable(String table)
	{
		this.query.append("DROP TABLE ");
		this.query.append(table);
	}

	/**
	 * 
	 * @param table
	 * @param values
	 */
	public void DeleteRowValues(String table, String[] values)
	{
		this.query.append("DELETE FROM ");
		this.query.append(table);
		this.query.append(" ");
		
		this.Where(values);
	}

	/**
	 * 
	 * @param table
	 * @param on
	 */
	public void InnerJoin(String table, String on)
	{
		//INNER JOIN	
		this.query.append("INNER JOIN ");
		this.query.append(this.database + "." +table);

		this.query.append(" ON ");
		this.query.append(on);
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
	public void execute( ) throws SQLException
	{
		PreparedStatement stmt = conn.prepareStatement(query.toString());		
		stmt.execute();

		this.query = null;
		this.query = new StringBuilder();
	}

	/**
	 * 
	 * @return
	 * @throws SQLException
	 */
	public ResultSet executeQuery( ) throws SQLException
	{		
		PreparedStatement stmt = conn.prepareStatement(this.query.toString());		
		ResultSet result =  stmt.executeQuery();

		this.query = new StringBuilder();

		return result;
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
		
		this.query = new StringBuilder(sql);
		
		ResultSet result =  this.executeQuery();	
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
	 * 
	 * @return
	 */
	public Connection getConnection( )
	{
		return conn;
	}

	/**
	 * 
	 * @return
	 */
	public String getQuery( )
	{
		return query.toString();
	}
}
