using System;
using System.Configuration;
using System.Data;

using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;

namespace DataAccess
{
	/// <summary>
	/// Summary description for OracleDB.
	/// </summary>
	public class OracleDB
	{
		public OracleDB()
		{
			//
			// TODO: Add constructor logic here
			//
		}
		public static OracleConnection GetConnection(string strConnString)
		{
			OracleConnection conn = null;
			try
			{
				if (strConnString==null || strConnString.Length == 0)
					throw new Exception("Connection String is null");
				else
					conn = new OracleConnection(strConnString);
			}
			catch (OracleException e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				throw new SystemException("Database error, please contact system administrator");
			}

			return conn;
		}
		private static void SetDefaultLongFetchSize(ref OracleCommand cmd)
		{
			int initialLongFetchSize;
			try
			{
				if (System.Configuration.ConfigurationSettings.AppSettings["InitialLONGFetchSize"] != null)
				{
					initialLongFetchSize = Convert.ToInt32(System.Configuration.ConfigurationSettings.AppSettings["InitialLONGFetchSize"]);
				} 
				else
				{
					initialLongFetchSize = 128000;
				}
			}
			catch
			{
				initialLongFetchSize = 128000;
			}
			cmd.InitialLONGFetchSize = initialLongFetchSize;
		}
		public static OracleCommand CreateCommand()
		{
			OracleCommand cmd = null;
			try
			{
				cmd = new OracleCommand();
				SetDefaultLongFetchSize(ref cmd);
				string BindByNameSetting = 
					ConfigurationSettings.AppSettings["ODP_BindByName"];
				cmd.BindByName = (BindByNameSetting.ToLower() == "true");
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				throw new SystemException("Database error, please contact system administrator");
			}
			return cmd;
		}
		public static OracleCommand CreateCommand(string strCommandString)
		{
			OracleCommand cmd = null;
			try
			{
				cmd = new OracleCommand(strCommandString);
				SetDefaultLongFetchSize(ref cmd);
				string BindByNameSetting = 
					ConfigurationSettings.AppSettings["ODP_BindByName"];
				cmd.BindByName = (BindByNameSetting.ToLower() == "true");
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				throw new SystemException("Database error, please contact system administrator");
			}
			return cmd;
		}
		public static OracleCommand CreateCommand(string strCommandString,OracleConnection oraConn)
		{
			OracleCommand cmd = null;
			try
			{
				cmd = new OracleCommand(strCommandString, oraConn);
				SetDefaultLongFetchSize(ref cmd);
				string BindByNameSetting = 
					ConfigurationSettings.AppSettings["ODP_BindByName"];
				cmd.BindByName = (BindByNameSetting.ToLower() == "true");
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				throw new SystemException("Database error, please contact system administrator");
			}
			return cmd;
		}

		public static OracleDataAdapter GetDataAdapter()
		{
			OracleDataAdapter da = null;
			try 
			{
				da = new OracleDataAdapter();
			}
			catch (OracleException oe)
			{
				Logger.Append(oe.Source + " throws " +oe.Message);
				throw new SystemException("Database error, please contact system administrator");
			}
			return da;
		}
		public static OracleDataAdapter GetDataAdapter(string procName, OracleConnection conn)
		{
			OracleDataAdapter da = null;
			try 
			{
				da = new OracleDataAdapter(procName, conn);
			}
			catch (OracleException oe)
			{
				Logger.Append(oe.Source + " throws " +oe.Message);
				throw new SystemException("Database error, please contact system administrator");
			}
			return da;
		}
		public static OracleDataAdapter GetDataAdapter(string strConnect, string procName)
		{
			OracleDataAdapter da = null;
			
			try 
			{
				da = new OracleDataAdapter(procName, strConnect);
			}
			catch (OracleException oe)
			{
				Logger.Append(oe.Source + " throws " +oe.Message);
				throw new SystemException("Database error, please contact system administrator");
			}
			return da;
		}
		public static OracleDbType ToOracleType(DbType dbType)
		{
			switch (dbType)
			{
				case DbType.String:
					return OracleDbType.Varchar2;
				case DbType.Byte:
					return OracleDbType.Byte;
				case DbType.Binary:
					return OracleDbType.Raw;
				case DbType.Date:
					return OracleDbType.Date;
				case DbType.DateTime:
					return OracleDbType.TimeStamp;
				case DbType.Decimal:
					return OracleDbType.Decimal;
				case DbType.Double:
					return OracleDbType.Double;
				case DbType.Single:
					return OracleDbType.Single;
				case DbType.Int16:
					return OracleDbType.Int16;
				case DbType.Int32:
					return OracleDbType.Int32;
				case DbType.Int64:
					return OracleDbType.Int64;
				case DbType.StringFixedLength:
					return OracleDbType.Char;
				default:
					throw new SystemException("Unsupported Data type of Oracle Database");

			}
		}
	}
}
