using Renci.SshNet;
using Renci.SshNet.Sftp;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace FMSWosup
{
	public enum workType
	{
		[Description("FMS WO UPDATE")]
		update,
		[Description("FMS WO WOSUP")]
		wosup
	}

	class Program
	{
		private static int woupdWorkOrderCount { get; set; } = 0;
		private static int wosupWorkOrderCount { get; set; } = 0;
		private static SftpClient client { get; set; }
		private static Config config = new Config();

		

		static void Main(string[] args)
		{

			Logger.addLog("start program");

			try
			{
				if (Config.isTesting)
				{
					Logger.addLog("This execution is for testing, sending data to test server");
				}
				
				sendSFTP(workType.update);
				sendSMTP(workType.update);

				sendSFTP(workType.wosup);
				sendSMTP(workType.wosup);


				Logger.addLog(string.Format("[{0}]", workType.update.ToString()), "Finished Generating Data, Updating Database");
				Util.updateDataByType(workType.update);
				Logger.addLog(string.Format("[{0}]", workType.wosup.ToString()), "Finished Generating Data, Updating Database");
				Util.updateDataByType(workType.wosup);

				Logger.addLog("done");
				Logger.commit();
			}
			catch (Exception e)
			{
				Logger.addLog("Cannot complete works: error throwed:");
				Logger.addLog(e.Message);
				Logger.commit();
			}
}

		private static void sendSFTP(workType type)
		{

			Logger.addLog(string.Format("[{0}]",type.ToString()),"Establishing SFTP Connection");
			client = new SftpClient(Config.sftpHost, Config.sftpPort, Config.sftpUsername, Config.sftpPassword);
			client.Connect();

			Logger.addLog(string.Format("[{0}]", type.ToString()), "Generating Etime files");
			generateEtimeOutputFile(type);

			uploadFileToSFTP(type);

			Logger.addLog(string.Format("[{0}]", type.ToString()), "Disconnecting SFTP connection");
			client.Disconnect();
			
		}

		private static void sendSMTP(workType type)
		{
			Logger.addLog(string.Format("[{0}]", type.ToString()), "Generating SMTP connecting and email details");
			MailMessage message = new MailMessage
			{
				From = new MailAddress(Config.smtpFrom),
				Subject = Config.smtpSubject,
				Body = Config.getSMTPBody(Config.GetOutputSMTPFooterPrefixByType(type, getWorkOrderCountByType(type))),
				IsBodyHtml = true,
			};
			foreach(string to in Config.recipientEmailList)
			{
				message.To.Add(to);
			}
			SmtpClient smtpClient = new SmtpClient(Config.smtpServer);

			Logger.addLog(string.Format("[{0}]", type.ToString()), "Sending SMTP emails");
			smtpClient.Send(message);
			
		}

		private static void removeFileWithSameName(workType type)
		{
			string name = Config.GetOutputFileNameByType(type);
			foreach (SftpFile file in client.ListDirectory(Config.sftpTargetPath))
			{
				if (file.Name==name)
				{
					Logger.addLog("Removing", name, "in server because of duplication of same name");
					client.DeleteFile(file.FullName);
				}
			}
		}

		private static void uploadFileToSFTP(workType type)
		{
			if (getWorkOrderCountByType(type)<1)
			{
				return;
			}

			removeFileWithSameName(type);

			Logger.addLog("Uploading", Config.GetLocalFilePathByType(type), "to SFTP host as", Config.GetOutputFilePathByType(type));
			using (FileStream fs = File.OpenRead(Config.GetLocalFilePathByType(type)))
			{
				client.UploadFile(fs, Config.GetOutputFilePathByType(type), null);
			}
			
			
		}

		private static void generateEtimeOutputFile(workType type)
		{

			Logger.addLog(string.Format("[{0}]", type.ToString()), "fetching dataset from oracle database");
			DataSet ds = Util.getEtimeDataSetByType(type);
			int count = Util.getRowCountFromDataset(ds);
			setWorkOrderCountByType(type, count);
			if (count < 1)
			{
				Logger.addLog(string.Format("[{0}]", type.ToString()), "fetched dataset is empty, skip...");
			}
			else
			{
				using (StreamWriter sw = new StreamWriter(Config.GetLocalFilePathByType(type)))
				{
					Logger.addLog(string.Format("[{0}]", type.ToString()), "formating dataset and saving to local as", Config.GetLocalFilePathByType(type));
					Util.generateTextByDataset(ds, sw, Config.GetGeneratedPrefixLineByType(type));
					sw.WriteLine(Config.GetOutputFooterByType(type, count));
				}
			}
		}

		private static void setWorkOrderCountByType(workType type, int count)
		{
			switch (type)
			{
				case workType.update:
					woupdWorkOrderCount = count;
					break;
				case workType.wosup:
					wosupWorkOrderCount = count;
					break;
			}
		}

		private static int getWorkOrderCountByType(workType type)
		{
			switch (type)
			{
				case workType.update:
					return woupdWorkOrderCount;
				case workType.wosup:
					return wosupWorkOrderCount;
				default:
					return 0;
			}
		}
	}
}
