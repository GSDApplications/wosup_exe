using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Specialized;
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;
using System.Configuration;
using System.IO;
using DataAccess;

namespace FMSWosup
{
	class Util
	{
		public static void updateDataByType(workType type)
		{
			switch (type)
			{
				case workType.update:
					updateDataByProcedure("UPDATE_ETIME_WOUPD_UPLOAD");
					break;
				case workType.wosup:
					updateDataByProcedure("UPDATE_ETIME_WOSUP_UPLOAD");
					break;
			}
		}

		private static void updateDataByProcedure(string procedure)
		{
			DAccess db = new DAccess();
			OracleConnection conn = Config.isTesting ? db.tmpGetTestOracleConnection(): db.tmpGetOracleConnection();

			OracleCommand cmd = conn.CreateCommand();
			cmd.Connection.Open();
			cmd.CommandType = CommandType.StoredProcedure;
			cmd.CommandText = string.Format("{1}.FMS_WOSUP.{0}", procedure, Config.dbUser);
			cmd.ExecuteNonQuery();
			if (cmd.Connection.State != ConnectionState.Closed)
				cmd.Connection.Close();
		}

		private static DataSet getDataByProcedure(string procedure)
		{
			DAccess db = new DAccess();
			OracleConnection conn = Config.isTesting ? db.tmpGetTestOracleConnection() : db.tmpGetOracleConnection();

			OracleCommand cmd = conn.CreateCommand();
			cmd.CommandType = CommandType.StoredProcedure;
			cmd.CommandText = string.Format("{1}.FMS_WOSUP.{0}", procedure, Config.dbUser);

			DataSet ds = new DataSet();
			cmd.Parameters.Add(new OracleParameter("p_recordset", OracleDbType.RefCursor, ParameterDirection.Output));
			
			OracleDataAdapter da = new OracleDataAdapter(cmd);
			da.Fill(ds, "DataTable1");

			if (cmd.Connection.State != ConnectionState.Closed)
				cmd.Connection.Close();

			return ds;
		}

		public static DataSet getEtimeDataSetByType(workType type)
		{
			switch (type)
			{
				case workType.update:
					return getDataByProcedure("GET_ETIME_UPDATE");
				case workType.wosup:
					return getDataByProcedure("GET_ETIME_WOSUP");
				default:
					return getDataByProcedure("GET_ETIME_UPDATE");
			}
		}

		public static string generateTextLineByByteLength(string text, int byteLength)
		{
			List<string> chars = new List<string>();
			for(int i = 0; i < text.Length; i++)
			{
				chars.Add(text.Substring(i, 1));
			}
			for(int i = text.Length; i < byteLength; i++)
			{
				chars.Add(" ");
			}
			return string.Join("", chars);
		}

		public static void generateTextByDataset(DataSet ds, StreamWriter sw, string rowPrefix="")
		{
			foreach(DataTable dt in ds.Tables)
			{
				foreach(DataRow dr in dt.Rows)
				{
					string line = "";
					foreach(Config.OutputItem item in Config.etimeOutputList)
					{
						line += generateTextLineByByteLength(dr[item.value].ToString(), item.byteLength);
					}
					if (!string.IsNullOrEmpty(rowPrefix))
					{
						sw.WriteLine(rowPrefix);
					}
					sw.WriteLine(line);
				}
			}
		}

		public static int getRowCountFromDataset(DataSet ds)
		{
			int count = 0;
			foreach(DataTable dt in ds.Tables)
			{
				foreach(DataRow dr in dt.Rows)
				{
					count += 1;
				}
			}
			return count;
		}
	}
}
