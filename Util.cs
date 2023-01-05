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
		private static List<string> woupdWOIDList { get; set; } = new List<string>();
		private static List<string> wosupWOIDList { get; set; } = new List<string>();

		public static void updateDataByType(workType type, Action<string> log)
		{
			switch (type)
			{
				case workType.update:
					foreach(string id in woupdWOIDList)
					{
						log(id);
						updateDataByProcedure("UPDATE_ETIME_WOUPD_UPLOAD", id);
					}
					break;
				case workType.wosup:
					foreach(string id in wosupWOIDList)
					{
						log(id);
						updateDataByProcedure("UPDATE_ETIME_WOSUP_UPLOAD", id);
					}
					break;
			}
		}

		private static void updateDataByProcedure(string procedure, string id)
		{
			DAccess db = new DAccess();
			OracleConnection conn = Config.isTesting ? db.tmpGetTestOracleConnection(): db.tmpGetOracleConnection();

			OracleCommand cmd = conn.CreateCommand();
			cmd.Connection.Open();
			cmd.CommandType = CommandType.StoredProcedure;
			cmd.CommandText = string.Format("{1}.FMS_WOSUP.{0}", procedure, Config.dbUser);
			cmd.Parameters.Add(new OracleParameter("v_wo_id", OracleDbType.Varchar2, id, ParameterDirection.Input));
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

		public static string generateTextLineByByteLength(string text, int byteLength, bool fromLeft=true)
		{
			List<string> chars = new List<string>();
			for(int i = 0; i < text.Length; i++)
			{
				chars.Add(text.Substring(i, 1));
			}
			if (fromLeft)
			{
				for (int i = text.Length; i < byteLength; i++)
				{
					chars.Add(" ");
				}
				return string.Join("", chars);
			}
			else
			{
				List<string> empty = new List<string>();
				for (int i = text.Length; i < byteLength; i++)
				{
					empty.Add(" ");
				}
				return string.Join("", empty)+ string.Join("", chars);
			}
			
		}

		public static void generateTextByDataset(DataSet ds, StreamWriter sw, workType type)
		{
			foreach(DataTable dt in ds.Tables)
			{
				foreach(DataRow dr in dt.Rows)
				{
					string line = "";
					foreach(Config.OutputItem item in Config.getOutputListByType(type))
					{
						line += generateTextLineByByteLength(dr[item.value].ToString().Trim(), item.byteLength);
					}
					if (type==workType.wosup)
					{
						string prefix = "";
						foreach (Config.OutputItem item in Config.etimeHDRWosupOutputList)
						{
							prefix += generateTextLineByByteLength(dr[item.value].ToString().Trim(), item.byteLength);
						}
						sw.WriteLine(prefix);
					}
					sw.WriteLine(line);
				}
			}
		}

		public static int getRowCountFromDataset(DataSet ds, workType type)
		{
			int count = 0;
			foreach(DataTable dt in ds.Tables)
			{
				foreach(DataRow dr in dt.Rows)
				{
					count += 1;
					switch (type)
					{
						case workType.update:
							woupdWOIDList.Add(dr["FMIS_WO_ID"].ToString());
							break;
						case workType.wosup:
							wosupWOIDList.Add(dr["FMIS_WO_ID"].ToString());
							break;
					}
				}
			}
			return count;
		}
	}
}
