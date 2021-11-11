using Renci.SshNet;
using Renci.SshNet.Sftp;
using System;
using System.Collections.Generic;
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

		static void Main(string[] args)
		{
			workCount = 2;

			Logger.addLog("start program");

			try
			{
				sendSFTP();

				if (workCount > 0)
				{
					sendSMTP();
				}

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
			client = new SftpClient("10.32.195.163", 22, "gsdetimt", "gsdetimt1");
			client.Connect();

			Logger.addLog("Establish SFTP Connection");

			removeOldFiles();
			Logger.addLog("Remvove SFTP host target path old files");
			generateEtimeOutputFile();
			Logger.addLog("Saved generated Etime file to local");

			uploadFileAndRemoveLocal(Config.localWoupdFilePath, Config.outputWoupdFilePath);
			uploadFileAndRemoveLocal(Config.localWosupFilePath, Config.outputWosupFilePath);

			client.Disconnect();
			Logger.addLog("Disconnected SFTP connection");
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

			smtpClient.Send(message);
			Logger.addLog("Send email to target clients");
		}

		private static void removeOldFiles()
		{
			foreach (SftpFile file in client.ListDirectory(Config.sftpTargetPath))
			{
				if ((file.Name != ".") && (file.Name != ".."))
				{
					client.DeleteFile(file.FullName);					
				}
			}
		}

		private static void uploadFileAndRemoveLocal(string localPath, string targetPath)
		{
			if (!File.Exists(localPath))
			{
				return;
			}

			using (FileStream fs = File.OpenRead(localPath))
			{
				client.UploadFile(fs, targetPath, null);
			}
			Logger.addLog("Uploaded", localPath, "to SFTP host as", targetPath);
			File.Delete(localPath);
			Logger.addLog("Removed local file", localPath);
		}

		private static void generateEtimeOutputFile()
		{
			//WOUPD
			
			DataSet dsWoupd = Util.getEtimeUpdate();
			Logger.addLog("fetched woupd from oracle database");
			if (Util.IsEmpty(dsWoupd))
			{
				workCount -= 1;
				Logger.addLog("fetched woupd dataset is empty, skip...");
			}
			else
			{
				using (StreamWriter swWoupd = new StreamWriter(Config.localWoupdFilePath))
				{
					Util.generateTextByDataset(dsWoupd, swWoupd);
					swWoupd.WriteLine(Util.generateTextLineByByteLength(Config.outputWoupdFooter, Config.outputFooterByteLength));
					Logger.addLog("formated woupd and saved to local", Config.localWoupdFilePath);
				}
			}



			//WOSUP
			
			DataSet dsWosup = Util.getEtimeWosup();
			Logger.addLog("fetched wosup from oracle database");
			if (Util.IsEmpty(dsWosup))
			{
				workCount -= 1;
				Logger.addLog("fetched wosup dataset is empty, skip...");
			}
			else
			{
				using (StreamWriter swWosup = new StreamWriter(Config.localWosupFilePath))
				{
					Util.generateTextByDataset(dsWosup, swWosup, Config.etimePrefixLine);
					swWosup.WriteLine(Util.generateTextLineByByteLength(Config.outputWoupdFooter, Config.outputFooterByteLength));
					Logger.addLog("formated wosup and saved to local", Config.localWosupFilePath);
				}
			}
				
		}
	}
}
