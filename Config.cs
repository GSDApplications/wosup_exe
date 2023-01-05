using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FMSWosup
{
	public class Config
	{
		public class OutputItem
		{
			public string value;
			public int byteLength;
			public OutputItem(string value, int byteLength)
			{
				this.value = value;
				this.byteLength = byteLength;
			}
		}

		public static bool isTesting = bool.Parse(ConfigurationManager.AppSettings["isTesting"]);
		public static string dbUser = ConfigurationManager.AppSettings["dbUser"];

		// SFTP config
		public static string sftpHost = ConfigurationManager.AppSettings[isTesting? "sftpHostTest" : "sftpHost"];
		public static int sftpPort = int.Parse(ConfigurationManager.AppSettings["sftpPort"]);
		public static string sftpUsername = ConfigurationManager.AppSettings[isTesting? "sftpUsernameTest" : "sftpUsername"];
		public static string sftpPassword = ConfigurationManager.AppSettings[isTesting? "sftpPasswordTest" : "sftpPassword"];
		public static string sftpTargetPath = ConfigurationManager.AppSettings["sftpTargetPath"];

		// SMTP config
		public static string smtpServer = ConfigurationManager.AppSettings["smtpServer"];
		public static string smtpFrom = ConfigurationManager.AppSettings["smtpFrom"];
		public static string smtpSubject = "FMS Scheduled Job Executed";
		public static List<string> recipientEmailList = ConfigurationManager.AppSettings["smtpRecipientEmailList"].ToString().Split(',').ToList<string>().ConvertAll<string>(email=>email.Trim());

		public static string getSMTPBody(string prefix)
		{
			string smtpServerHtmlLink = string.Format("<a href={0}>{1}</a>", sftpHost, sftpHost);
			string smtpBodyFooterHtml = string.Format("<p>Notice from Scheduled Job at {0}</p>", DateTime.Now.ToString("dd-MMM-yy, hh:mm tt"));
			string smtpBodyContentHtml = string.Format("<p>{0} Work Orders sent to FMS server {1}</p>", prefix, smtpServerHtmlLink);
			return smtpBodyContentHtml+smtpBodyFooterHtml;
		}

		// output config
		public static string etimePrefixLine = "HDRWOSUP   40                              1OE 1";
		public static List<OutputItem> etimeUpdateOutputList = new List<OutputItem>
		{
			new OutputItem("WOR", 3),
			new OutputItem("WOUPD", 8),
			new OutputItem("UPD_NULL", 8),
			new OutputItem("FMIS_DEPT_NBR", 4),
			new OutputItem("FMIS_WO_ID", 8),
			new OutputItem("WO_NM", 60),
			new OutputItem("WO_SH_NM", 15),
			new OutputItem("CNTAC_CD", 18),
			new OutputItem("WO_OPEN_DT", 8),
			new OutputItem("WO_CLOSE_DT", 8),
			new OutputItem("PLAN_FRM_DT", 8),
			new OutputItem("PLAN_TO_DT", 8),
			new OutputItem("CAP_RATE_IND", 1),
			new OutputItem("WO_DI_IND", 1),
			new OutputItem("ACTV_WO_FLG", 1),
			new OutputItem("ALW_BUD_FL", 1),
			new OutputItem("FMIS_WO_DESC", 100),
			new OutputItem("WO_ORI_DEPT_NBR", 4),
			new OutputItem("WO_ORI_PROJ_ID" ,10),
			new OutputItem("RPT_1", 30),
			new OutputItem("RPT_2", 30),
			new OutputItem("RPT_3", 30),
			new OutputItem("RPT_4", 30),
			new OutputItem("RPT_5", 30),
			new OutputItem("RPT_6", 30),
		};

		public static List<OutputItem> etimeWosupOutputList = new List<OutputItem>
		{
			new OutputItem("WORWOSUP", 11),
			new OutputItem("FMIS_DEPT_NBR", 4),
			new OutputItem("EMPTY", 8),
			new OutputItem("EMPTY", 20),
			new OutputItem("DOC_VERS_TSKORD_LN_NO", 1),
			new OutputItem("DOC_VERS_TSKORD_LN_NO", 1),
			new OutputItem("FMIS_DEPT_NBR", 4),
			new OutputItem("FMIS_WO_ID", 8),
			new OutputItem("WO_NM", 60),
			new OutputItem("WO_SH_NM", 15),
			new OutputItem("EMPTY", 1),
			new OutputItem("CNTAC_CD", 18),
			new OutputItem("WO_OPEN_DT", 8),
			new OutputItem("WO_CLOSE_DT", 8),
			new OutputItem("PLAN_FRM_DT", 8),
			new OutputItem("PLAN_TO_DT", 8),
			new OutputItem("CAP_RATE_IND", 1),
			new OutputItem("WO_DI_IND", 1),
			new OutputItem("EMPTY", 1),
			new OutputItem("ALW_BUD_FL", 1),
			new OutputItem("FMIS_WO_DESC", 100),
			new OutputItem("FMS_PROJECT_ID", 10),
			new OutputItem("WO_MAJ_PROJ_CD", 6),
			new OutputItem("PHASE_CD", 6),
			new OutputItem("ACTV_CD", 4),
			new OutputItem("LOC_CD", 4),
			new OutputItem("FUNC_CD", 10),
			new OutputItem("WO_ORI_DEPT_NBR", 4),
			new OutputItem("WO_ORI_PROJ_ID" ,10),
			new OutputItem("RPT_1", 30),
			new OutputItem("RPT_2", 30),
			new OutputItem("RPT_3", 30),
			new OutputItem("RPT_4", 30),
			new OutputItem("RPT_5", 30),
			new OutputItem("RPT_6", 30),
		};

		public static List<OutputItem> etimeHDRWosupOutputList = new List<OutputItem>
		{
			new OutputItem("HDRWOSUP", 11),
			new OutputItem("FMIS_DEPT_NBR", 4),
			new OutputItem("EMPTY", 8),
			new OutputItem("EMPTY", 20),
			new OutputItem("DOC_VERS_TSKORD_LN_NO", 1),
			new OutputItem("OE", 3),
			new OutputItem("DOC_VERS_TSKORD_LN_NO", 1),
			new OutputItem("EMPTY", 60),
			new OutputItem("EMPTY", 8),
			new OutputItem("EMPTY", 4),
			new OutputItem("EMPTY", 3),
			new OutputItem("EMPTY", 60),
			new OutputItem("EMPTY", 250),
		};

		public static List<OutputItem> getOutputListByType(workType type)
		{
			switch (type)
			{
				case workType.update:
					return etimeUpdateOutputList;
				case workType.wosup:
					return etimeWosupOutputList;
				default:
					return etimeUpdateOutputList;
			}
		}

		private static string localFilePath = ConfigurationManager.AppSettings["localFilePath"].ToString();

		private static string woupdTitle = ConfigurationManager.AppSettings["woupdFileName"].ToString();
		private static string wosupTitle = ConfigurationManager.AppSettings["wosupFileName"].ToString();

		public static string outputWoupdFileName = string.Format("{1}_{0}.txt", DateTime.Now.ToString("yyyyMMdd"), woupdTitle);
		public static string outputWosupFileName = string.Format("{1}_{0}.txt", DateTime.Now.ToString("yyyyMMdd"), wosupTitle);

		public static string localWoupdFilePath = string.Format("{0}{1}", localFilePath, outputWoupdFileName);
		public static string localWosupFilePath = string.Format("{0}{1}", localFilePath, outputWosupFileName);

		public static string outputWoupdFilePath = string.Format("{0}/{1}", sftpTargetPath, outputWoupdFileName);
		public static string outputWosupFilePath = string.Format("{0}/{1}", sftpTargetPath, outputWosupFileName);
		
		public static string outputWoupdFooter = string.Format("TRL{0}", outputWoupdFileName);
		public static string outputWosupFooter = string.Format("TRL{0}", outputWosupFileName);
		public static int outputFooterByteLength = 32;
		public static string getOutputFooter(string fileName, int workOrderCount, int headerCount=0)
		{
			return string.Format("TRL{0}{1}{2}{3}", Util.generateTextLineByByteLength(fileName, 32), Util.generateTextLineByByteLength(workOrderCount.ToString(), 10, false), headerCount == 0 ? "" : Util.generateTextLineByByteLength(headerCount.ToString(), 10, false), Util.generateTextLineByByteLength("", 36));
		}


		//log path
		public static string localLogFilePath = ConfigurationManager.AppSettings["localLogFilePath"];
		public static string logFileName = string.Format("{1}_{0}", DateTime.Now.ToString("MM_dd_yyyy_hh_mm_ss"), ConfigurationManager.AppSettings["localLogFileName"]);
		public static string logFileFullNamePath = string.Format("{0}/{1}.txt", localLogFilePath, logFileName);

		

		// get by type
		public static string GetLocalFilePathByType(workType type)
		{
			switch (type)
			{
				case workType.update:
					return localWoupdFilePath;
				case workType.wosup:
					return localWosupFilePath;
				default:
					return localWoupdFilePath;
			}
		}

		public static string GetOutputFilePathByType(workType type)
		{
			switch (type)
			{
				case workType.update:
					return outputWoupdFilePath;
				case workType.wosup:
					return outputWosupFilePath;
				default:
					return outputWoupdFilePath;
			}
		}

		public static string GetOutputFileNameByType(workType type)
		{
			switch (type)
			{
				case workType.update:
					return outputWoupdFileName;
				case workType.wosup:
					return outputWosupFileName;
				default:
					return outputWoupdFileName;
			}
		}

		public static string GetGeneratedPrefixLineByType(workType type)
		{
			switch (type)
			{
				case workType.update:
					return string.Empty;
				case workType.wosup:
					return etimePrefixLine;
				default:
					return string.Empty;
			}
		}

		public static string GetOutputFooterByType(workType type, int count)
		{
			switch (type)
			{
				case workType.update:
					return getOutputFooter(outputWoupdFileName, count + 1);
				case workType.wosup:
					return getOutputFooter(outputWosupFileName, count * 2 + 1, count);
				default:
					return string.Empty;
			}
		}

		public static string GetOutputSMTPFooterPrefixByType(workType type, int count)
		{
			switch (type)
			{
				case workType.update:
					return string.Format("{0} updated", count);
				case workType.wosup:
					return count.ToString();
				default:
					return string.Empty;
			}
		}
	}
}
