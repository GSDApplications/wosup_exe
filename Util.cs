using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Collections.Specialized;
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;
using System.IO;

namespace FMSWosup
{
	class Util
	{
		private static OracleConnection conn = new OracleConnection(ConfigurationSettings.AppSettings["dbConnection"]);

		private static DataSet getDataByProcedure(string procedure)
		{
			OracleCommand cmd = conn.CreateCommand();
			cmd.CommandType = CommandType.StoredProcedure;
			cmd.CommandText = string.Format("SSYU.FMS_WOSUP.{0}", procedure);

			DataSet ds = new DataSet();
			cmd.Parameters.Add(new OracleParameter("p_recordset", OracleDbType.RefCursor, ParameterDirection.Output));
			
			OracleDataAdapter da = new OracleDataAdapter(cmd);
			da.Fill(ds, "DataTable1");

			if (cmd.Connection.State != ConnectionState.Closed)
				cmd.Connection.Close();

			return ds;
		}

		public static DataSet getEtimeUpdate()
		{
			return getDataByProcedure("GET_ETIME_UPDATE");
		}

		public static DataSet getEtimeWosup()
		{
			return getDataByProcedure("GET_ETIME_WOSUP");
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

		public static bool IsEmpty(DataSet dataSet)
		{
			foreach (DataTable table in dataSet.Tables)
				if (table.Rows.Count != 0)
				{
					Logger.addLog(table.Rows.Count);
					return false;
				}

			return true;
		}
	}
}
