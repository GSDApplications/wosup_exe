using System;
using System.Data;
using System.Runtime.Serialization;

namespace DataAccess
{
	/// <summary>
	/// Summary description for TemplateParameter.
	/// </summary>
	public class TemplateParameter : IDataParameter
	{
		DbType m_dbType  = DbType.Object;
		ParameterDirection m_direction = ParameterDirection.Input;
		bool m_fNullable  = false;
		string m_sParamName;
		string m_sSourceColumn;
		DataRowVersion m_sourceVersion = DataRowVersion.Current;
		object m_value;

		public TemplateParameter()
		{
		}

		public TemplateParameter(string parameterName, DbType type)
		{
			m_sParamName = parameterName;
			m_dbType   = type;
		}

		public TemplateParameter(string parameterName, object value)
		{
			m_sParamName = parameterName;
			this.Value = value;
			// Setting the value also infers the type.
		}

		public TemplateParameter( string parameterName, DbType dbType, object value )
		{
			m_sParamName  = parameterName;
			m_dbType    = dbType;
			this.Value = value;
		}

		public DbType DbType 
		{
			get  { return m_dbType; }
			set  { m_dbType = value;  }
		}

		public ParameterDirection Direction 
		{
			get { return m_direction; }
			set { m_direction = value; }
		}

		public Boolean IsNullable 
		{
			get { return m_fNullable; }
		}

		public String ParameterName 
		{
			get { return m_sParamName; }
			set { m_sParamName = value; }
		}

		public String SourceColumn 
		{
			get { return m_sSourceColumn; }
			set { m_sSourceColumn = value; }
		}

		public DataRowVersion SourceVersion 
		{
			get { return m_sourceVersion; }
			set { m_sourceVersion = value; }
		}

		public object Value 
		{
			get
			{
				return m_value;
			}
			set
			{
				m_value    = value;
				if (m_dbType == DbType.Object)
				{
					m_dbType  = _inferType(value);
				}
			}
		}

		/// <summary>
		/// Lets you set a param value to null easier, if you expect it might be
		/// </summary>
		/// <param name="value"></param>
		/// <returns>false - value was null, true - value was not null</returns>
		public bool SetOrNull(object value)
		{
			if ((value == null) || (value == DBNull.Value)
				|| (value is string && (value == null || (value as string) == string.Empty))
				|| (value is int && (value == null || ((int)value) == 0))
				|| (value is DateTime && (value == null || ((DateTime)value) == DateTime.MinValue))
				|| (value is decimal && (value == null || ((decimal)value) == 0))
				|| (value is char && (value == null || ((char)value) == 0x00))
				)
			{
				Value = DBNull.Value;
				return false;
			} else {
				Value = value;
				return true;
			}
		}

		private DbType _inferType(Object value)
		{
			switch (Type.GetTypeCode(value.GetType()))
			{
				case TypeCode.Object:
					return DbType.Object;

				case TypeCode.Char:
					return DbType.StringFixedLength;

				case TypeCode.UInt16:
					return DbType.UInt16;

				case TypeCode.UInt32:
					return DbType.UInt32;

				case TypeCode.UInt64:
					return DbType.UInt64;

				case TypeCode.Boolean:
					return DbType.Boolean;

				case TypeCode.Byte:
					return DbType.Byte;

				case TypeCode.Int16:
					return DbType.Int16;

				case TypeCode.Int32:
					return DbType.Int32;

				case TypeCode.Int64:
					return DbType.Int64;

				case TypeCode.Single:
					return DbType.Single;

				case TypeCode.Double:
					return DbType.Double;

				case TypeCode.Decimal:
					return DbType.Decimal;

				case TypeCode.DateTime:
					return DbType.DateTime;

				case TypeCode.String:
					return DbType.String;

				case TypeCode.Empty:
				case TypeCode.DBNull:
				case TypeCode.SByte:
					// Throw a SystemException for unsupported data types.
					throw new SystemException("Invalid data type");

				default:
					throw new SystemException("Value is of unknown data type");
			}
		}
	}

}
