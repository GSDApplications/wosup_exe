using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;

namespace DataAccess
{
	/// <summary>
	/// Enumeration for database provider, now catains OleDb, SqlClient, and OracleClient
	/// </summary>
	public enum ProviderType
	{
		OleDb = 0, SqlClient, OracleClient, ODBC
	};
	/// <summary>
	/// Generic class for data access
	/// </summary>
	public class DAccess
	{
		private ProviderType _provider;
		bool allowErrors = ConfigurationSettings.AppSettings["allowErrors"] == "true";
		const string sGenericError = "Database error, please contact system administrator";

		public DAccess(ProviderType providerType)
		{
			_provider = providerType;
		}
		/// <summary>
		/// if no specific data provider, try read from db, if not, use OleDb
		/// </summary>
		public DAccess()
		{
			string dbType = ConfigurationSettings.AppSettings["dbType"];
			switch (dbType.ToLower())
			{
				case "oracle":
					_provider = ProviderType.OracleClient;
					break;
				case "sqlserver":
					_provider = ProviderType.SqlClient;
					break;
				case "odbc":
					_provider = ProviderType.ODBC;
					break;
				default:
					_provider = ProviderType.OleDb;
					break;
			}
		}
		/// <summary>
		/// Method to Get connection to database
		/// </summary>
		/// <returns>IDbConnection</returns>
		public IDbConnection GetConnection()
		{
			if (GetConnStringAppSetting("dbConnection") == null)
				throw new ApplicationException("Default database connection not defined");
			string strConnString = GetConnStringAppSetting("dbConnection");
			if (strConnString.Length == 0)
				throw new ApplicationException("Default database connection not defined");

			switch (_provider)
			{
				case ProviderType.OracleClient:
					return OracleDB.GetConnection(strConnString);
				case ProviderType.ODBC:
					return ODBCDB.GetConnection(strConnString);
				case ProviderType.OleDb:
					break;
				case ProviderType.SqlClient:
					break;
			}
			return null;
		}



        ///EY - Added Connection for Oracle
        /// <summary>
        /// Method to Get connection to database
        /// </summary>
        /// <returns>IDbConnection</returns>
        public OracleConnection tmpGetOracleConnection()
        {
            if (GetConnStringAppSetting("dbConnection") == null)
                throw new ApplicationException("Default database connection not defined");
            string strConnString = GetConnStringAppSetting("dbConnection");
            if (strConnString.Length == 0)
                throw new ApplicationException("Default database connection not defined");
            return new OracleConnection(strConnString);
        }


		public OracleConnection tmpGetFMSCROracleConnection()
		{
			if (GetConnStringAppSetting("fmscr_dbConnection") == null)
				throw new ApplicationException("Default database connection not defined");
			string strConnString = GetConnStringAppSetting("fmscr_dbConnection");
			if (strConnString.Length == 0)
				throw new ApplicationException("Default database connection not defined");
			return new OracleConnection(strConnString);
		}


		/// <summary>
		/// Method to Get connection to database
		/// </summary>
		/// <param name="strConnString">A string contains database connection information</param>
		/// <returns>IDbConnection</returns>
		public IDbConnection GetConnection(string strConnString)
		{
			switch (_provider)
			{
				case ProviderType.OracleClient:
					return OracleDB.GetConnection(strConnString);
				case ProviderType.ODBC:
					return ODBCDB.GetConnection(strConnString);
				case ProviderType.OleDb:
					break;
				case ProviderType.SqlClient:
					break;
			}
			return null;
		}
		private string GetConnStringAppSetting(string name)
		{
			string encrypted_setting = ConfigurationSettings.AppSettings[name + "_encrypted"];
			bool encrypted = encrypted_setting != null && encrypted_setting != string.Empty &&
			                 Convert.ToBoolean(encrypted_setting);
			if (!encrypted)
			{
				return ConfigurationSettings.AppSettings[name];
			} else
			{
				string[] connStringParts = ConfigurationSettings.AppSettings[name].Split(';');
				for (int i=0; i<connStringParts.Length;i++)
				{
					string segment = connStringParts[i];
					if (segment.ToLower().StartsWith("password=") || segment.ToLower().StartsWith("pass=") || segment.ToLower().StartsWith("pwd="))
					{
						string decryptedpassword = string.Empty;
						int idxBreak = connStringParts[i].IndexOf('=');
						if (idxBreak > -1)
						{
							decryptedpassword = new GSD.Cryptography.SimpleEncryption("eNcr|ptedG$D").DecryptString(connStringParts[i].Substring(idxBreak + 1));
						}
						connStringParts[i] = string.Format("Password={0}", decryptedpassword);
					} 
				}
				return string.Join(";", connStringParts);
			}
		}
		/// <summary>
		/// Method to create a database command
		/// </summary>
		/// <returns>IDbCommandS</returns>
		public IDbCommand CreateCommand()
		{
			switch(_provider)
			{
				case ProviderType.OracleClient:
					return OracleDB.CreateCommand();
				case ProviderType.ODBC:
					return ODBCDB.CreateCommand();
				case ProviderType.OleDb:
					break;
				case ProviderType.SqlClient:
					break;
			}
			return null;
		}
		/// <summary>
		/// Method to create a database command
		/// </summary>
		/// <param name="strCommand">A string contains database command</param>
		/// <param name="connDB">Database connection of IDbConnection</param>
		/// <returns>IDbCommand</returns>
		public IDbCommand CreateCommand(string strCommand, IDbConnection connDB)
		{
			switch(_provider)
			{
				case ProviderType.OracleClient:
					return OracleDB.CreateCommand(strCommand, (OracleConnection)connDB);
				case ProviderType.ODBC:
					return ODBCDB.CreateCommand(strCommand, (OdbcConnection)connDB);
				case ProviderType.OleDb:
					break;
				case ProviderType.SqlClient:
					break;
			}
			return null;
		}
		public IDbDataParameter CreateParameter(string paraName, DbType paraType, ParameterDirection paraDirection)
		{
			switch(_provider)
			{
				case ProviderType.OracleClient:
					OracleParameter ora_parameter = new OracleParameter(paraName, OracleDB.ToOracleType(paraType));
					ora_parameter.Direction = paraDirection;
					return ora_parameter;
				case ProviderType.ODBC:
					OdbcParameter odbc_parameter = new OdbcParameter(paraName, ODBCDB.ToOdbcType(paraType));
					odbc_parameter.Direction = paraDirection;
					return odbc_parameter;
				case ProviderType.OleDb:
					break;
				case ProviderType.SqlClient:
					break;
			}
			return null;
		}
		/// <summary>
		/// Method to create a database command
		/// </summary>
		/// <param name="strCommand">A string contains database command</param>
		/// <returns>IDbCommand</returns>
		public IDbCommand CreateCommand(string strCommand)
		{
			switch (_provider)
			{
				case ProviderType.OracleClient:
					return OracleDB.CreateCommand(strCommand);
				case ProviderType.ODBC:
					return ODBCDB.CreateCommand(strCommand);
				case ProviderType.OleDb:
					break;
				case ProviderType.SqlClient:
					break;
			}
			return null;
		}

		public IDataReader GetDataReader(IDbConnection conn, string procName, params TemplateParameter[] ParamList)
		{
			IDbCommand cmd = null;
			try
			{
				cmd = this.CreateCommand(procName, conn);
				if (ParamList != null
					&& ParamList.Length > 0)
				{
					foreach (TemplateParameter p in ParamList)
					{
						IDataParameter oParam = this.CreateParameter(p.ParameterName, p.DbType, p.Direction);
						oParam.Value = p.Value;
						cmd.Parameters.Add(oParam);
					}
				}
				cmd.Connection.Open();
				return cmd.ExecuteReader(CommandBehavior.CloseConnection);
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " + e.Message);
				if (allowErrors)
				{
					throw e;
				}
				else
				{
					throw new SystemException(sGenericError);
				}
			}
		} 

		public IDataReader GetDataReader(IDbConnection conn,string procName) 
		{
			return GetDataReader(conn, procName, null);
		} 
		/// <summary>
		/// Method to return a Database Reader
		/// </summary>
		/// <param name="strConnect">A string contains database connection information</param>
		/// <param name="procName">A string contains dabase command, including query statement or procedure name</param>
		/// <returns>IDataReader</returns>
		/// <remarks>It is not suggested to use because of performance issue and implementation details</remarks>
		public IDataReader GetDataReader(string strConnect,string procName) 
		{
			return GetDataReader(this.GetConnection(strConnect), procName);
		} 
		/// <summary>
		/// Method to return a DataSet 
		/// </summary>
		/// <param name="conn">IDbConnection to a database</param>
		/// <param name="ProcName">String array which contains multiple sql statement or procedure </param>
		/// <param name="DataTable">String array which contains name of the tables in returned dataset</param>
		/// <returns>DataSet</returns>
		public DataSet GetDataSet(IDbConnection conn, string[] ProcName , string[] DataTable)
		{
			DataSet ds = new DataSet();
			try
			{
				IDbDataAdapter da = this.GetDataAdapter();
				for (int i = 0; i < ProcName.Length; i ++)
				{
					da.SelectCommand = this.CreateCommand(ProcName[i], conn);
					da.Fill(ds);
					DataTable dt = ds.Tables["Table"];
					if (dt != null)
						dt.TableName = DataTable[i];
				}
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} else
				{
					throw new SystemException(sGenericError);
				}
			}
			return ds;
		}
		/// <summary>
		/// Method to return a DataSet 
		/// </summary>
		/// <param name="strConnect">A string contains database connection information</param>
		/// <param name="ProcName">String array which contains multiple sql statement or procedure</param>
		/// <param name="DataTable">String array which contains name of the tables in returned dataset</param>
		/// <returns>DataSet</returns>
		public DataSet GetDataSet(string strConnect, string[] ProcName , string[] DataTable)
		{
			DataSet ds = new DataSet();
			try
			{
				IDbConnection conn = this.GetConnection(strConnect);
				IDbDataAdapter da = this.GetDataAdapter();
				for (int i = 0; i < ProcName.Length; i ++)
				{
					da.SelectCommand = this.CreateCommand(ProcName[i], conn);
					da.Fill(ds);
					DataTable dt = ds.Tables["Table"];
					if (dt != null)
						dt.TableName = DataTable[i];
				}
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
			return ds;
		}
		/// <summary>
		/// Method to return a DataSet 
		/// </summary>
		/// <param name="strConnect">A string contains database connection information</param>
		/// <param name="ProcName">String array which contains only the names of multiple sql statement or procedure</param>
		/// <param name="ParamList">ArrayList array which contains list of params to pass into the query (but not procedures) list must match ProcName</param>
		/// <param name="DataTable">String array which contains name of the tables in returned dataset</param>
		/// <param name="SQLFolderPath">String with the location of the SQL queries</param>
		/// <returns>DataSet</returns>
		public DataSet GetDataSet(string strConnect, string[] ProcName, ArrayList[] ParamList, string[] DataTable, string SQLFolderPath)
		{
			DataSet ds = new DataSet();
			try
			{
				//error if the folder does not exist that is passed in
				if (! System.IO.Directory.Exists(SQLFolderPath))
				{
					throw new SystemException(string.Format("SQLFolderPath: {0}", SQLFolderPath));
				}
				// ensure a trailing slash
				SQLFolderPath = (SQLFolderPath[SQLFolderPath.Length-1] == '\\') ? SQLFolderPath : SQLFolderPath +"\\";
				// add the folder name of the ProviderType, and a slash
				SQLFolderPath = SQLFolderPath +_provider.ToString() +"\\";
				// error if the folder does not exist with the ProviderType tacked on
				if (! System.IO.Directory.Exists(SQLFolderPath))
				{
					throw new SystemException(string.Format("SQLFolderPath: {0}", SQLFolderPath));
				}

				// check all the ProcName items passed in to ensure they exist as files.
				for (int i = 0; i < ProcName.Length; i++)
				{
					string sFile = SQLFolderPath +ProcName[i] +".sql";
					if (! System.IO.File.Exists(sFile))
					{
						throw new SystemException(string.Format("ProcName: {0} does not exist in SQLFolderPath {1}", ProcName[i], SQLFolderPath));
					}
				}
				IDbConnection conn = this.GetConnection(strConnect);
				IDbDataAdapter da = this.GetDataAdapter();
				for (int i = 0; i < ProcName.Length; i ++)
				{
					string sFile = SQLFolderPath +ProcName[i] +".sql";

					// get the arraylist into an array of objects since this is what string.Format uses
					ArrayList tmplist = ParamList[0];
					object[] mylist = new object[tmplist.Count];
					for (int j=0;j<tmplist.Count;j++)
					{
						mylist[j] = tmplist[j];
						// if the item is of type string, ensure it's safe (ie no unescaped single quotes)
						if ((mylist[j]).GetType() == Type.GetType("System.String")) 
						{
							mylist[j] = ((string)mylist[j]).Replace("'", "''");
						}
					}
					System.IO.StreamReader sr = new System.IO.StreamReader(sFile);
					try
					{
						string sqltext = string.Format(sr.ReadToEnd(), mylist);

						da.SelectCommand = this.CreateCommand(sqltext, conn);
						da.Fill(ds);
						DataTable dt = ds.Tables["Table"];
						if (dt != null)
							dt.TableName = DataTable[i];
					}
					finally
					{
						sr.Close();
					}
				}
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
			return ds;
		}
		#region GetDataSetSql and overloads
		/// <summary>
		/// Method to return a DataSet 
		/// </summary>
		/// <param name="query">the select sql string to run</param>
		/// <param name="DataTable">String which contains name of the table in returned dataset</param>
		/// <param name="ParamList">Array of params</param>
		/// <returns>DataSet</returns>
		public DataSet GetDataSetSql(string query, string DataTable, params TemplateParameter[] ParamList)
		{
			return GetDataSetSql(GetConnection(), query, DataTable, ParamList);
		}
		/// <summary>
		/// Method to return a DataSet 
		/// </summary>
		/// <param name="query">the select sql string to run</param>
		/// <param name="DataTable">String which contains name of the table in returned dataset</param>
		/// <param name="ParamList">Collection of params</param>
		/// <returns>DataSet</returns>
		public DataSet GetDataSetSql(string query, string DataTable, TemplateCollection ParamList)
		{
			return GetDataSetSql(GetConnection(), query, DataTable, (TemplateParameter[])(ParamList.ToArray(typeof(TemplateParameter))));
		}
		/// <summary>
		/// Method to return a DataSet 
		/// </summary>
		/// <param name="conn">A db connection</param>
		/// <param name="query">the select sql string to run</param>
		/// <param name="DataTable">DataTable to be returned</param>
		/// <param name="ParamList">Collection of params</param>
		/// <returns>DataSet</returns>
		public DataSet GetDataSetSql(IDbConnection conn, string query, string DataTable, TemplateCollection ParamList)
		{
			return GetDataSetSql(conn, query, DataTable, (TemplateParameter[])(ParamList.ToArray(typeof(TemplateParameter))));
		}
		/// <summary>
		/// Method to return a DataSet 
		/// </summary>
		/// <param name="conn">A db connection</param>
		/// <param name="query">the select sql string to run</param>
		/// <param name="DataTable">DataTable to be returned</param>
		/// <param name="ParamList">Array of params</param>
		/// <returns>DataSet</returns>
		public DataSet GetDataSetSql(IDbConnection conn, string query, string DataTable, params TemplateParameter[] ParamList)
		{
			DataSet ds = new DataSet();
			try
			{
				IDbDataAdapter da = this.GetDataAdapter();
			
				switch (_provider)
				{
					case ProviderType.OracleClient:
					case ProviderType.ODBC:
						break;
					default:
						throw new NotImplementedException("This provider type is not implemented");
				}
				da.SelectCommand = this.CreateCommand(query, conn);
				if (ParamList != null
					&& ParamList.Length > 0)
				{
					foreach (TemplateParameter p in ParamList)
					{
						IDbDataParameter oParam = this.CreateParameter(p.ParameterName, p.DbType, p.Direction);
						oParam.Direction = p.Direction;
						oParam.Value = p.Value;
						da.SelectCommand.Parameters.Add(oParam);
					}
				}
				da.Fill(ds);
				DataTable dt = ds.Tables["Table"];
				if (dt != null)
					dt.TableName = DataTable;
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
			return ds;
		}
		#endregion GetDataSetSql and overloads

		public int RunSql(string query, params TemplateParameter[] ParamList)
		{
			return RunSql(GetConnection(), query, ParamList);
		}
		public int RunSql(string query, TemplateCollection ParamList)
		{
			return RunSql(GetConnection(), query, (TemplateParameter[])(ParamList.ToArray(typeof(TemplateParameter))));
		}
		public int RunSql(IDbConnection conn, string query, params TemplateParameter[] ParamList)
		{
			try
			{
				switch (_provider)
				{
					case ProviderType.OracleClient:
					case ProviderType.ODBC:
						break;
					default:
						throw new NotImplementedException("This provider type is not implemented");
				}
				IDbCommand cmd = this.CreateCommand(query, conn);
				if (ParamList != null
					&& ParamList.Length > 0)
				{
					foreach (TemplateParameter p in ParamList)
					{
						IDataParameter oParam = this.CreateParameter(p.ParameterName, p.DbType, p.Direction);
						oParam.Value = p.Value;
						cmd.Parameters.Add(oParam);
					}
				}
				int result;
				if (conn.State != ConnectionState.Open)
				{
					conn.Open();
					try
					{
						result = cmd.ExecuteNonQuery();						
					} 
					finally
					{
						conn.Close();
					}
				} 
				else
				{
					result = cmd.ExecuteNonQuery();						
				}
				return result;
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
		}
		public int RunSql(IDbConnection conn, string query, TemplateCollection ParamList)
		{
			return RunSql(conn, query, (TemplateParameter[])(ParamList.ToArray(typeof(TemplateParameter))));
		}
		/// <summary>
		/// Method to return IDbDataAdapter
		/// </summary>
		/// <returns>IDbDataAdapter</returns>
		public IDbDataAdapter GetDataAdapter()
		{
			try
			{
				switch (_provider)
				{
					case ProviderType.OracleClient:
						return OracleDB.GetDataAdapter();
					case ProviderType.ODBC:
						return ODBCDB.GetDataAdapter();
					case ProviderType.OleDb:
						break;
					case ProviderType.SqlClient:
						break;
				}
				return null;
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
		}
		/// <summary>
		/// Method to return IDbDataAdapter
		/// </summary>
		/// <param name="procName">A string contains database connection information</param>
		/// <param name="connDB">Database connection of IDbConnection</param>
		/// <returns>IDbDataAdapter</returns>
		public IDbDataAdapter GetDataAdapter(string procName, IDbConnection connDB)
		{
			try
			{
				switch (_provider)
				{
					case ProviderType.OracleClient:
						return OracleDB.GetDataAdapter(procName, connDB as OracleConnection);
					case ProviderType.ODBC:
						return ODBCDB.GetDataAdapter(procName, connDB as OdbcConnection);
					case ProviderType.OleDb:
						break;
					case ProviderType.SqlClient:
						break;
				}
				return null;
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
		}
		/// <summary>
		/// Method to return IDbDataAdapter
		/// </summary>
		/// <param name="strConnect">A string contains database connection information</param>
		/// <param name="procName">A string contains dabase command, including query statement or procedure name</param>
		/// <returns>IDbDataAdapter</returns>
		/*public IDbDataAdapter GetDataAdapter(string strConnect, string procName)
		{
		IDbDataAdapter da = null;
		try
		{
		if(_provider == ProviderType.OracleClient)
		{
		da = OracleDB.GetDataAdapter(strConnect, procName);
		}
		}
		catch (Exception e)
		{
		Logger.Append(e.Source + " throws " +e.Message);
		throw new SystemException(sGenericError);
		}
		return da;
		}*/
		public IDbDataAdapter GetDataAdapter(string strConnect, string procName)
		{
			try
			{
				switch (_provider)
				{
					case ProviderType.OracleClient:
						return OracleDB.GetDataAdapter(strConnect, procName);
					case ProviderType.ODBC:
						return ODBCDB.GetDataAdapter(strConnect, procName);
					case ProviderType.OleDb:
						break;
					case ProviderType.SqlClient:
						break;
				}
				return null;
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
		}


		/// <summary>
		/// Method to return a DataView
		/// </summary>
		/// <param name="strConnect">A string contains database connection information</param>
		/// <param name="procName">A string contains database command, including nonquery statement or procedure name</param>
		/// <param name="dataSetTable">String array which contains name of the tables in returned dataset</param>
		/// <returns>DataView</returns>
		public DataView GetDataView  ( string strConnect,string procName,string dataSetTable)
		{          
			DataView objDataView = null;
			if (_provider == ProviderType.OracleClient)
			{	
				try
				{
					IDbConnection conn = this.GetConnection(strConnect);
					conn.Open();
					try
					{
						//open the connection and execute the command
						IDbDataAdapter da = this.GetDataAdapter(procName, conn);
						DataSet ds = new DataSet(dataSetTable);
						da.Fill(ds);
						DataTable dt = ds.Tables["Table"];
						return (dt != null)
							? new DataView(dt)
							: null;
					}
					finally
					{
						conn.Close();
					}
				}
				catch (Exception e)
				{
					Logger.Append(e.Source + " throws " +e.Message);
					if (allowErrors)
					{
						throw e;
					} 
					else
					{
						throw new SystemException(sGenericError);
					}
				}
			}
			return objDataView;
		}

		private static int ExecProc(IDbCommand cmd, IDbConnection conn)
		{
			int retval;
			if (conn.State != ConnectionState.Open)
			{
				conn.Open();
				try
				{
					retval = cmd.ExecuteNonQuery();						
				} 
				finally
				{
					conn.Close();
				}
			} 
			else
			{
				retval = cmd.ExecuteNonQuery();						
			}
			return retval;
		}

		public int RunProc(IDbConnection conn,CommandType commandType, string procName, TemplateCollection commandParams)
		{
			int retval = 0;
			try 
			{
				IDbCommand cmd = this.CreateCommand(procName, conn);
				cmd.CommandType = commandType;
				if (commandParams !=null)
				{
					foreach (TemplateParameter param in commandParams)
					{
						IDataParameter oParam = this.CreateParameter(param.ParameterName, param.DbType, param.Direction);
						oParam.Value = param.Value;
						cmd.Parameters.Add(oParam);
					}
				}
				retval = ExecProc(cmd, conn);
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
			return retval;
		}

		/// <summary>
		/// Method to execute stored procudure
		/// </summary>
		/// <param name="strConnect">A string contains database connection information</param>
		/// <param name="commandType">CommandType to specify whether the command is a stored procedure or statement</param>
		/// <param name="procName">A string contains dabase command, including nonquery statement or procedure name</param>
		/// <param name="commandParams">Zero or more of TemplateParameter for the parameters in stored procedure</param>
		/// <returns>Return code of executing stored procedure</returns>
		public int RunProc(string strConnect,CommandType commandType, string procName, TemplateCollection commandParams)
		{   
			int retval = 0;
			try 
			{
				IDbConnection conn = this.GetConnection(strConnect);
				retval = RunProc(conn, commandType, procName, commandParams);
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
			return retval;
		}
		
		public int RunProc(IDbConnection conn,CommandType commandType, string procName, ref TemplateCollection commandParams)
		{   
			int retval = 0;
			try 
			{
				IDbCommand cmd = this.CreateCommand(procName, conn);
				cmd.CommandType  = commandType;
				if (commandParams !=null)
				{
					foreach (TemplateParameter param in commandParams)
					{
						IDataParameter oParam = this.CreateParameter(param.ParameterName, param.DbType, param.Direction);
						oParam.Value = param.Value;
						cmd.Parameters.Add(oParam);
					}
				}
				retval = ExecProc(cmd, conn);
				if (commandParams !=null)
				{
					foreach (TemplateParameter param in commandParams)
					{
						if ((param.Direction == ParameterDirection.InputOutput)
							|| (param.Direction == ParameterDirection.Output))
						{
							param.Value = (cmd.Parameters[param.ParameterName] as IDataParameter).Value;
						}
					}
				}
				cmd.Parameters.Clear();
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
			return retval;
		}

		/// <summary>
		/// Method to execute stored procudure
		/// </summary>
		/// <param name="strConnect">A string contains database connection information</param>
		/// <param name="commandType">CommandType to specify whether the command is a stored procedure or statement</param>
		/// <param name="procName">A string contains dabase command, including nonquery statement or procedure name</param>
		/// <param name="commandParams">Zero or more of TemplateParameter for the parameters in stored procedure</param>
		/// <returns>Return code of executing stored procedure</returns>
		public int RunProc(string strConnect,CommandType commandType, string procName, ref TemplateCollection commandParams)
		{   
			int retval = 0;
			try 
			{
				IDbConnection conn = this.GetConnection(strConnect);
				retval = RunProc(conn, commandType, procName, commandParams);
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
			return retval;
		}

		public int RunProc(IDbConnection conn,CommandType commandType, string procName, params TemplateParameter[] commandParams)
		{   
			int retval = 0;
			try 
			{
				IDbCommand cmd = this.CreateCommand(procName, conn);
				cmd.CommandType  = commandType;
				if (commandParams !=null)
					foreach (TemplateParameter param in commandParams)
					{
						IDataParameter oParam = this.CreateParameter(param.ParameterName, param.DbType, param.Direction);
						oParam.Value = param.Value;
						cmd.Parameters.Add(oParam);
					}
				retval = ExecProc(cmd, conn);
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
			return retval;
		}
		/// <summary>
		/// Method to execute stored procudure
		/// </summary>
		/// <param name="strConnect">A string contains database connection information</param>
		/// <param name="commandType">CommandType to specify whether the command is a stored procedure or statement</param>
		/// <param name="procName">A string contains dabase command, including nonquery statement or procedure name</param>
		/// <param name="commandParams">Zero or more of TemplateParameter for the parameters in stored procedure</param>
		/// <returns>Return code of executing stored procedure</returns>
		public int RunProc(string strConnect,CommandType commandType, string procName, params TemplateParameter[] commandParams)
		{   
			int retval = 0;
			try 
			{
				IDbConnection conn = this.GetConnection(strConnect);
				retval = RunProc(conn, commandType, procName, commandParams);
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
			return retval;
		}

		public int RunProc(IDbConnection conn,CommandType commandType, string procName, ref TemplateParameter[] commandParams)
		{   
			int retval = 0;
			try 
			{
				IDbCommand cmd = this.CreateCommand(procName, conn);
				cmd.CommandType  = commandType;
				if (commandParams !=null)
					foreach (TemplateParameter param in commandParams)
					{
						IDataParameter oParam = this.CreateParameter(param.ParameterName, param.DbType, param.Direction);
						oParam.Value = param.Value;
						cmd.Parameters.Add(oParam);
					}
				retval = ExecProc(cmd, conn);
				if (commandParams !=null)
				{
					foreach (TemplateParameter param in commandParams)
					{
						if ((param.Direction == ParameterDirection.InputOutput)
							|| (param.Direction == ParameterDirection.Output))
						{
							param.Value = (cmd.Parameters[param.ParameterName] as IDataParameter).Value;
						}
					}
				}
				cmd.Parameters.Clear();
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
			return retval;
		}
		/// <summary>
		/// Method to execute stored procudure
		/// </summary>
		/// <param name="strConnect">A string contains database connection information</param>
		/// <param name="commandType">CommandType to specify whether the command is a stored procedure or statement</param>
		/// <param name="procName">A string contains dabase command, including nonquery statement or procedure name</param>
		/// <param name="commandParams">Zero or more of TemplateParameter for the parameters in stored procedure</param>
		/// <returns>Return code of executing stored procedure</returns>
		public int RunProc(string strConnect,CommandType commandType, string procName, ref TemplateParameter[] commandParams)
		{   
			int retval = 0;
			try 
			{
				IDbConnection conn = this.GetConnection(strConnect);
				retval = RunProc(conn, commandType, procName, commandParams);
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
			return retval;
		}
		public int RunProc(IDbConnection conn,string procName)
		{
			return RunProc(conn,CommandType.Text,procName,(TemplateParameter[])null );
		}
		/// <summary>
		/// Method to execute stored procudure
		/// </summary>
		/// <param name="strConnect">A string contains database connection information</param>
		/// <param name="procName">A string contains nonquery statement</param>
		/// <returns>Return code of executing stored procedure</returns>
		public int RunProc(string strConnect,string procName)
		{
			return RunProc(strConnect,CommandType.Text,procName,(TemplateParameter[])null );
		}
		public int RunProc(IDbConnection conn,CommandType commandtype, string procName)
		{
			return RunProc(conn, commandtype, procName, (TemplateParameter[])null);
		}
		/// <summary>
		/// Method to execute stored procudure
		/// </summary>
		/// <param name="strConnect">A string contains database connection information</param>
		/// <param name="commandtype">CommandType to specify whether the command is a stored procedure or statement</param>
		/// <param name="procName">A string contains dabase command, including nonquery statement or procedure name</param>
		/// <returns>Return code of executing stored procedure</returns>
		public int RunProc(string strConnect,CommandType commandtype, string procName)
		{
			return RunProc(strConnect, commandtype, procName, (TemplateParameter[])null);
		}
		
		public object GetObject(IDbConnection conn,string procName) 
		{
			return GetObject(conn, CommandType.Text , procName, null ) ;
		}
		/// <summary>
		/// Returns the first column of the first row in the result set  
		/// </summary>
		/// <param name="strConnect">A string contains database connection information</param>
		/// <param name="procName">A string contains dabase command, including nonquery statement or procedure name</param>
		/// <returns>Object</returns>
		public object GetObject(string strConnect,string procName) 
		{
			return GetObject(strConnect, CommandType.Text , procName, null ) ;
		}
		public object GetObject(IDbConnection conn, CommandType commandType, string procName) 
		{
			return GetObject(conn, commandType , procName, null ) ;
		}
		/// <summary>
		/// Returns the first column of the first row in the result set  
		/// </summary>
		/// <param name="strConnect">A string contains database connection information</param>
		/// <param name="commandType">CommandType to specify whether the command is a stored procedure or statement</param>
		/// <param name="procName">A string contains dabase command, including nonquery statement or procedure name</param>
		/// <returns>Object</returns>
		public object GetObject(string strConnect, CommandType commandType, string procName) 
		{
			return GetObject(strConnect, commandType , procName, null ) ;
		}
		/// <summary>
		/// Returns the first column of the first row in the result set
		/// </summary>
		/// <param name="strConnect">A string contains database connection information</param>
		/// <param name="commandType">CommandType to specify whether the command is a stored procedure or statement</param>
		/// <param name="procName">CommandType to specify whether the command is a stored procedure or statement</param>
		/// <param name="commandParams">Zero or more of TemplateParameter for the parameters in stored procedure</param>
		/// <returns>Object</returns>
		public object GetObject(string strConnect, CommandType commandType, string procName, params TemplateParameter[] commandParams)
		{
			return GetObject(GetConnection(strConnect), commandType, procName, commandParams);
		}
		public object GetObject(IDbConnection conn, CommandType commandType, string procName, params TemplateParameter[] commandParams)
		{
			object obj = null ; 
			try 
			{
				IDbCommand cmd = this.CreateCommand(procName, conn);
				cmd.CommandType  = commandType;
				if (commandParams !=null)
					foreach (TemplateParameter param in commandParams)
					{
						IDataParameter oParam = this.CreateParameter(param.ParameterName, param.DbType, param.Direction);
						oParam.Value = param.Value;
						cmd.Parameters.Add(oParam);
					}
				conn.Open();
				obj = cmd.ExecuteScalar();
				cmd.Parameters.Clear();
				conn.Close();
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				if (allowErrors)
				{
					throw e;
				} 
				else
				{
					throw new SystemException(sGenericError);
				}
			}
			return obj;
		}
		public static string GetInList(int[] ids)
		{
			if (ids == null || ids.Length == 0)
			{
				return string.Empty;
			}
			StringBuilder result = new StringBuilder();
			int i = 0;
			do
			{
				result.Append(ids[i].ToString());
			} while (++i<ids.Length && (result.Append(",") != null));
			return result.ToString();
		}
		public static string GetInList(string[] ids)
		{
			if (ids == null || ids.Length == 0)
			{
				return string.Empty;
			}
			StringBuilder result = new StringBuilder();
			int i = 0;
			do
			{ // escape single quotes, and enquote item using single quotes.
				result.Append("'" +ids[i].Replace("'","''") +"'");
			} while (++i<ids.Length && (result.Append(",") != null));
			return result.ToString();
		}
	}
}
