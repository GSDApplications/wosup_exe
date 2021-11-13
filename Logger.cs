using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FMSWosup
{
	class Logger
	{
		private static List<string> logs = new List<string>();
		public static void addLog(params object[] argsRest)
		{
			string log = DateTime.Now + " ";
			foreach (var arg in argsRest)
			{
				log += arg + " ";
			}
			logs.Add("---"+log);
			Console.WriteLine("---" + log);
		}
		public static void commit()
		{
			Directory.CreateDirectory(Config.localLogFilePath);
			File.WriteAllText(Config.logFileFullNamePath, string.Join("\n", logs));
		}
	}
}
