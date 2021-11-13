using System;
using System.IO;


namespace DataAccess
{
	///<summary>
	///A class to keep error log record
	///</summary>
	
	public class Logger 
	{
		private static StreamWriter sw;
		private static String logDirectory;
			
		/// <summary>
		/// static constructor for Logger class
		/// </summary>
		/// <remarks>this class is not thread-safe</remarks>
		static Logger() 
		{
			// load the logging path once and store in the 'logDirectory' class var
			if (System.Configuration.ConfigurationSettings.AppSettings["logger"] != null)
			{
				logDirectory = System.Configuration.ConfigurationSettings.AppSettings["logger"].ToString();
				if (logDirectory.Length == 0) 
				{
					sw = null;
				}
			} 
			else
			{
				logDirectory = "C:\\";
				DirectoryInfo logdir = new DirectoryInfo(logDirectory);
				if (!logdir.Exists)
				{
					logDirectory = "C:\\";
				}
				logDirectory += "logger.txt";
			}
			//logDirectory = System.Configuration.ConfigurationSettings.AppSettings["logger"].ToString();
			// if the file doesn't exist, create it
			if (!File.Exists(logDirectory)) 
			{
				FileStream fs = File.Create(logDirectory);
				fs.Close();
			}			
		}
		
		/// <summary>
		/// A method to append the log file with exception information
		/// </summary>
		/// <param name="message">The string to write to the log file</param>
					
		public static void Append(String message) 
		{
			try 
			{
				// open up the streamwriter for writing..
				sw = File.AppendText(logDirectory);
				try
				{
					lock(sw) 
					{
						if (sw == null) { return; }
						sw.Write("\r\nLog Entry : ");
						sw.WriteLine("{0} : ", DateTime.Now.ToString("hh:mm:ss MM/dd/yyyy"));
						sw.WriteLine("  :");
						sw.WriteLine("  :{0}", message);
						sw.WriteLine ("-------------------------------");
						sw.Flush();
					}
				}
				finally
				{
					sw.Close();
				}
			} 
			catch (Exception e)  
			{
				throw new SystemException("Unexpected exception with logging file");
			}
		}
	}
}


