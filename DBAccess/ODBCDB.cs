using System;
using System.Data;
using System.Data.Odbc;

namespace DataAccess
{
	/// <summary>
	/// Summary description for ODBCDB.
	/// </summary>
	public class ODBCDB
	{
		public ODBCDB()
		{
			//
			// TODO: Add constructor logic here
			//
		}
		public static OdbcConnection GetConnection(string strConnString)
		{
			OdbcConnection conn = null;
			try
			{
				if (strConnString==null || strConnString.Length == 0)
					throw new Exception("Connection String is null");
				else
					conn = new OdbcConnection(strConnString);
			}
			catch (OdbcException e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				throw new SystemException("Database error, please contact system administrator");
			}

			return conn;
		}

		public static OdbcCommand CreateCommand()
		{
			OdbcCommand cmd = null;
			try
			{
				cmd = new OdbcCommand();
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				throw new SystemException("Database error, please contact system administrator");
			}
			return cmd;
		}
		public static OdbcCommand CreateCommand(string strCommandString)
		{
			OdbcCommand cmd = null;
			try
			{
				cmd = new OdbcCommand(strCommandString);
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				throw new SystemException("Database error, please contact system administrator");
			}
			return cmd;
		}
		public static OdbcCommand CreateCommand(string strCommandString,OdbcConnection odbcConn)
		{
			OdbcCommand cmd = null;
			try
			{
				cmd = new OdbcCommand(strCommandString, odbcConn);
			}
			catch (Exception e)
			{
				Logger.Append(e.Source + " throws " +e.Message);
				throw new SystemException("Database error, please contact system administrator");
			}
			return cmd;
		}

		public static OdbcDataAdapter GetDataAdapter()
		{
			OdbcDataAdapter da = null;
			try 
			{
				da = new OdbcDataAdapter();
			}
			catch (OdbcException oe)
			{
				Logger.Append(oe.Source + " throws " +oe.Message);
				throw new SystemException("Database error, please contact system administrator");
			}
			return da;
		}
		public static OdbcDataAdapter GetDataAdapter(string procName, OdbcConnection conn)
		{
			OdbcDataAdapter da = null;
			try 
			{
				da = new OdbcDataAdapter(procName, conn);
			}
			catch (OdbcException oe)
			{
				Logger.Append(oe.Source + " throws " +oe.Message);
				throw new SystemException("Database error, please contact system administrator");
			}
			return da;
		}
		public static OdbcDataAdapter GetDataAdapter(string strConnect, string procName)
		{
			OdbcDataAdapter da = null;
			
			try 
			{
				da = new OdbcDataAdapter(procName, strConnect);
			}
			catch (OdbcException oe)
			{
				Logger.Append(oe.Source + " throws " +oe.Message);
				throw new SystemException("Database error, please contact system administrator");
			}
			return da;
		}
		public static OdbcType ToOdbcType(DbType dbType)
		{
			switch (dbType)
			{
				case DbType.String:
					return OdbcType.VarChar;
				case DbType.Byte:
					return OdbcType.TinyInt;
				case DbType.Binary:
					return OdbcType.Binary;
				case DbType.Date:
					return OdbcType.Date;
				case DbType.DateTime:
					return OdbcType.DateTime;
				case DbType.Decimal:
					return OdbcType.Decimal;
				case DbType.Double:
					return OdbcType.Double;
				case DbType.Single:
					return OdbcType.Real;
				case DbType.Int16:
					return OdbcType.SmallInt;
				case DbType.Int32:
					return OdbcType.Int;
				case DbType.Int64:
					return OdbcType.BigInt;
				case DbType.StringFixedLength:
					return OdbcType.Char;
				default:
					throw new SystemException("Unsupported Data type of Odbc Database");

			}
		}
	
	
	}
}
