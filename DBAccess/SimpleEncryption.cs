using System;
using System.Security.Cryptography;
using System.Text;

namespace GSD.Cryptography
{
	/// <summary>
	/// Summary description for SimpleEncryption.
	/// </summary>
	public class SimpleEncryption
	{
		private string enPass; 
		public SimpleEncryption(string passWord)
		{
			enPass = passWord; 
		}
		private byte[] GetKey()
		{
			// instead of directly using the password as the key
			// hash it first
			return new MD5CryptoServiceProvider().ComputeHash(ASCIIEncoding.ASCII.GetBytes(enPass));
		}
		public string EncryptString(string inString)
		{
			byte[] buff;
			TripleDESCryptoServiceProvider des = new TripleDESCryptoServiceProvider();

			des.Key = this.GetKey();
			des.Mode = CipherMode.ECB; //CBC, CFB
			buff = ASCIIEncoding.ASCII.GetBytes(inString);

			// encrypt and return
			return Convert.ToBase64String(
				des.CreateEncryptor().TransformFinalBlock(buff, 0, buff.Length)
				);
		}
		public string DecryptString(string enString)
		{
			byte[] buff;
			TripleDESCryptoServiceProvider des = new TripleDESCryptoServiceProvider();

			buff = Convert.FromBase64String(enString);

			des.Key = this.GetKey();
			des.Mode = CipherMode.ECB; //CBC, CFB

			//decrypt DES 3 encrypted byte buffer and return ASCII string
			return ASCIIEncoding.ASCII.GetString(
				des.CreateDecryptor().TransformFinalBlock(buff, 0, buff.Length)
				);
		}
	}
}
