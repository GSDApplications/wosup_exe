using Renci.SshNet;
using Renci.SshNet.Sftp;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace FMSWosup
{
	class Program
	{
		private static int workCount { get; set; }
		private static SftpClient client { get; set; }
		private static Config config = new Config();

		static void Main(string[] args)
		{
			workCount = 2;

			Logger.addLog("start program");

			try
			{
				if (Config.isTesting)
				{
					Logger.addLog("This execution is for testing, running reset database");
					Util.updateDataResetWorkOrder();
				}
				

				sendSFTP();

				if (workCount > 0)
				{
					sendSMTP();
				}

				Logger.addLog("updating database WO REC_MODF");
				Util.updateDataWOUpdate();
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

		private static void sendSFTP()
		{

			Logger.addLog("Establishing SFTP Connection");
			client = new SftpClient(Config.sftpHost, Config.sftpPort, Config.sftpUsername, Config.sftpPassword);
			client.Connect();

			Logger.addLog("Generating Etime files");
			generateEtimeOutputFile();

			uploadFileToSFTP(Config.localWoupdFilePath, Config.outputWoupdFilePath);
			uploadFileToSFTP(Config.localWosupFilePath, Config.outputWosupFilePath);

			Logger.addLog("Disconnecting SFTP connection");
			client.Disconnect();
			
		}

		private static void sendSMTP()
		{
			
			MailMessage message = new MailMessage
			{
				From = new MailAddress(Config.smtpFrom),
				Subject = Config.smtpSubject,
				Body = Config.getSMTPBody(workCount),
				IsBodyHtml = true,
			};
			foreach(string to in Config.recipientEmailList)
			{
				message.To.Add(to);
			}
			SmtpClient smtpClient = new SmtpClient(Config.smtpServer);

			Logger.addLog("Sending email to target clients");
			smtpClient.Send(message);
			
		}

		private static void removeFileWithSameName(string name)
		{
			foreach (SftpFile file in client.ListDirectory(Config.sftpTargetPath))
			{
				if (file.Name==name)
				{
					Logger.addLog("Removing", name, "in server");
					client.DeleteFile(file.FullName);
				}
			}
		}

		private static void uploadFileToSFTP(string localPath, string targetPath)
		{
			if (!File.Exists(localPath))
			{
				return;
			}

			removeFileWithSameName(targetPath);

			Logger.addLog("Uploading", localPath, "to SFTP host as", targetPath);
			using (FileStream fs = File.OpenRead(localPath))
			{
				client.UploadFile(fs, targetPath, null);
			}
			
			
		}

		private static void generateEtimeOutputFile()
		{
			//WOUPD

			Logger.addLog("fetching woupd from oracle database");
			DataSet dsWoupd = Util.getEtimeUpdate();
			
			if (Util.IsEmpty(dsWoupd))
			{
				workCount -= 1;
				Logger.addLog("fetched woupd dataset is empty, skip...");
			}
			else
			{
				using (StreamWriter swWoupd = new StreamWriter(Config.localWoupdFilePath))
				{
					Logger.addLog("formating woupd and saving to local", Config.localWoupdFilePath);
					Util.generateTextByDataset(dsWoupd, swWoupd);
					swWoupd.WriteLine(Util.generateTextLineByByteLength(Config.outputWoupdFooter, Config.outputFooterByteLength));
				}
			}



			//WOSUP
			Logger.addLog("fetching wosup from oracle database");
			DataSet dsWosup = Util.getEtimeWosup();
			
			if (Util.IsEmpty(dsWosup))
			{
				Logger.addLog("fetched wosup dataset is empty, skip...");
				workCount -= 1;
			}
			else
			{
				using (StreamWriter swWosup = new StreamWriter(Config.localWosupFilePath))
				{
					Logger.addLog("formated wosup and saving to local", Config.localWosupFilePath);
					Util.generateTextByDataset(dsWosup, swWosup, Config.etimePrefixLine);
					swWosup.WriteLine(Util.generateTextLineByByteLength(Config.outputWoupdFooter, Config.outputFooterByteLength));
				}
			}
				
		}
	}
}
