using System;
using System.Data;

namespace DataAccess
{
	/// <summary>
	/// Provides SQL grouping features to disconnected recordsets.
	/// </summary>
	public class DataSetHelper
	{
		public DataSet ds;
		private System.Collections.ArrayList m_FieldInfo; private string m_FieldList;
		private System.Collections.ArrayList GroupByFieldInfo; private string GroupByFieldList;

		public DataSetHelper(ref DataSet DataSet)
		{
			ds = DataSet;
		}
		public DataSetHelper()
		{
			ds = null;
		}
		private class FieldInfo
		{
			public string RelationName;
			public string FieldName;	//source table field name
			public string FieldAlias;	//destination table field name
			public string Aggregate;
		}	
		/// <summary>
		/// This code parses FieldList into FieldInfo objects and then
		/// adds them to the m_FieldInfo private member
		/// </summary>
		/// <param name="FieldList">Systax:  
		/// [relationname.]fieldname[ alias], ...</param>
		/// <param name="AllowRelation"></param>
		private void ParseFieldList(string FieldList, bool AllowRelation)
		{
			if (m_FieldList == FieldList) return;
			m_FieldInfo = new System.Collections.ArrayList();
			m_FieldList = FieldList;
			FieldInfo Field; string[] FieldParts; string[] Fields=FieldList.Split(',');
			int i;
			for (i=0; i<=Fields.Length-1; i++)
			{
				Field=new FieldInfo();
				//parse FieldAlias
				FieldParts = Fields[i].Trim().Split(' ');
				switch (FieldParts.Length)
				{
					case 1:
						//to be set at the end of the loop
						break;
					case 2:
						Field.FieldAlias=FieldParts[1];
						break;
					default:
						throw new Exception("Too many spaces in field definition: '" + Fields[i] + "'.");
				}
				//parse FieldName and RelationName
				FieldParts = FieldParts[0].Split('.');
				switch (FieldParts.Length)
				{
					case 1:
						Field.FieldName=FieldParts[0];
						break;
					case 2:
						if (AllowRelation==false)
							throw new Exception("Relation specifiers not permitted in field list: '" + Fields[i] + "'.");
						Field.RelationName = FieldParts[0].Trim();
						Field.FieldName=FieldParts[1].Trim();
						break;
					default:
						throw new Exception("Invalid field definition: " + Fields[i] + "'.");
				}
				if (Field.FieldAlias==null)
					Field.FieldAlias = Field.FieldName;
				m_FieldInfo.Add (Field);
			}
		}	
		/// <summary>
		/// Parses FieldList into FieldInfo objects and adds them to the GroupByFieldInfo private member
		/// </summary>
		/// <param name="FieldList">FieldList syntax: 
		/// fieldname[ alias]|operatorname(fieldname)[ alias],...
		/// Supported Operators: count,sum,max,min,first,last</param>
		/// <remarks></remarks>
		private void ParseGroupByFieldList(string FieldList)
		{
			if (GroupByFieldList == FieldList) return;
			GroupByFieldInfo = new System.Collections.ArrayList();
			FieldInfo Field; string[] FieldParts; string[] Fields = FieldList.Split(',');
			for (int i=0; i<=Fields.Length-1;i++)
			{
				Field = new FieldInfo();
				//Parse FieldAlias
				FieldParts = Fields[i].Trim().Split(' ');
				switch (FieldParts.Length)
				{
					case 1:
						//to be set at the end of the loop
						break;
					case 2:
						Field.FieldAlias = FieldParts[1];
						break;
					default:
						throw new ArgumentException("Too many spaces in field definition: '" + Fields[i] + "'.");
				}
				//Parse FieldName and Aggregate
				FieldParts = FieldParts[0].Split('(');
				switch (FieldParts.Length)
				{
					case 1:
						Field.FieldName = FieldParts[0];
						break;
					case 2:
						Field.Aggregate = FieldParts[0].Trim().ToLower();    //we're doing a case-sensitive comparison later
						Field.FieldName = FieldParts[1].Trim(' ', ')');
						break;
					default:
						throw new ArgumentException("Invalid field definition: '" + Fields[i] + "'.");
				}
				if (Field.FieldAlias==null)
				{
					if (Field.Aggregate==null)
						Field.FieldAlias=Field.FieldName;
					else
						Field.FieldAlias = Field.Aggregate + "of" + Field.FieldName;
				}
				GroupByFieldInfo.Add(Field);
			}
			GroupByFieldList = FieldList;
		}
		/// <summary>
		/// Creates a table based on aggregates of fields of another table
		/// </summary>
		/// <param name="TableName"></param>
		/// <param name="SourceTable"></param>
		/// <param name="FieldList">Syntax: 
		/// fieldname[ alias]|aggregatefunction(fieldname)[ alias], ...</param>
		/// <returns></returns>
		/// <remarks>RowFilter affects rows before GroupBy operation. 
		/// No "Having" support though this can be emulated by subsequent 
		/// filtering of the table that results.</remarks>
		public DataTable CreateGroupByTable(string TableName, DataTable SourceTable, string FieldList)
		{
			if (FieldList == null)
			{
				throw new ArgumentException("You must specify at least one field in the field list.");
				//return CreateTable(TableName, SourceTable);
			}
			else
			{
				DataTable dt = new DataTable(TableName);
				ParseGroupByFieldList(FieldList);
				foreach (FieldInfo Field in GroupByFieldInfo)
				{
					if (Field.FieldName == "null")
					{
						DataColumn dc  = SourceTable.Columns[Field.FieldAlias];
						dt.Columns.Add(Field.FieldAlias, dc.DataType, dc.Expression);
					} 
					else
					{
						DataColumn dc  = SourceTable.Columns[Field.FieldName];
						if (Field.Aggregate==null)
							dt.Columns.Add(Field.FieldAlias, dc.DataType, dc.Expression);
						else
							dt.Columns.Add(Field.FieldAlias, dc.DataType);
					}
				}
				if (ds != null)
					ds.Tables.Add(dt);
				return dt;
			}
		}
		/// <summary>
		/// Looks up a FieldInfo record based on FieldName
		/// </summary>
		/// <param name="FieldList"></param>
		/// <param name="Name"></param>
		/// <returns></returns>
		private FieldInfo LocateFieldInfoByName(System.Collections.ArrayList FieldList, string Name)
		{
			foreach (FieldInfo Field in FieldList)
			{
				if (Field.FieldName==Name)
					return Field;
			}
			return null;
		}

		/// <summary>
		/// Compares two values to see if they are equal. Also compares DBNULL.Value.
		/// </summary>
		/// <param name="a"></param>
		/// <param name="b"></param>
		/// <returns></returns>
		/// <remarks>If your DataTable contains object fields, you must extend this
		/// function to handle them in a meaningful way if you intend to 
		/// group on them</remarks>
		private bool ColumnEqual(object a, object b)
		{
			if ((a is DBNull) && (b is DBNull))
				return true;    //both are null
			if ((a is DBNull) || (b is DBNull))
				return false;    //only one is null
			return (a.Equals(b));    //value type standard comparison
		}

		/// <summary>
		/// Returns MIN of two values - DBNull is less than all others
		/// </summary>
		/// <param name="a"></param>
		/// <param name="b"></param>
		/// <returns></returns>
		private object Min(object a, object b)
		{
			if ((a is DBNull) || (b is DBNull))
				return DBNull.Value;
			if (((IComparable)a).CompareTo(b)==-1)
				return a;
			else
				return b;
		}

		/// <summary>
		/// Returns Max of two values - DBNull is less than all others
		/// </summary>
		/// <param name="a"></param>
		/// <param name="b"></param>
		/// <returns></returns>
		private object Max(object a, object b)
		{
			if (a is DBNull)
				return b;
			if (b is DBNull)
				return a;
			if (((IComparable)a).CompareTo(b)==1)
				return a;
			else
				return b;
		}

		/// <summary>
		/// Adds two values - if one is DBNull, then returns the other
		/// </summary>
		/// <param name="a"></param>
		/// <param name="b"></param>
		/// <returns></returns>
		private object Add(object a, object b)
		{
			if (a is DBNull)
				return b;
			if (b is DBNull)
				return a;
			return ((decimal)a + (decimal)b);
		}
		/// <summary>
		/// Selects data from one DataTable to another and performs
		/// various aggregate functions along the way. 
		/// See <see cref="InsertGroupByInto"/>
		/// and <see cref="ParseGroupByFieldList"/> for supported 
		/// aggregate functions.
		/// </summary>
		/// <param name="TableName"></param>
		/// <param name="SourceTable"></param>
		/// <param name="FieldList">Syntax: 
		/// fieldname[ alias]|aggregatefunction(fieldname)[ alias], ...</param>
		/// <param name="RowFilter"></param>
		/// <param name="GroupBy"></param>
		/// <returns></returns>
		public DataTable SelectGroupByInto(string TableName, DataTable SourceTable, string FieldList, string RowFilter, string GroupBy, bool RollUp)
		{
			DataTable dt = CreateGroupByTable(TableName, SourceTable, FieldList);
			InsertGroupByInto(dt, SourceTable, FieldList, RowFilter, GroupBy, RollUp);
			return dt;
		}
		/// <summary>
		/// Copies the selected rows and columns from SourceTable 
		/// and inserts them into DestTable. NOTICE this method has a bug
		/// If you want to roll-up, you must have one extra column which can
		/// be all null, and put this in your GroupBy, but do not add this
		/// fake field to your FieldList, and you will get the desired result.
		/// </summary>
		/// <param name="DestTable"></param>
		/// <param name="SourceTable"></param>
		/// <param name="FieldList">Syntax: 
		/// fieldname[ alias]|aggregatefunction(fieldname)[ alias], ...</param>
		/// <param name="RowFilter"></param>
		/// <param name="GroupBy"></param>
		public void InsertGroupByInto(DataTable DestTable, DataTable SourceTable, string FieldList, string RowFilter, string GroupBy, bool Rollup)
		{
			if (!Rollup)
			{
				InsertGroupByInto(DestTable, SourceTable, FieldList, RowFilter, GroupBy);
				return;
			}
			//'
			//' Copies the selected rows and columns from SourceTable and inserts them into DestTable
			//' FieldList has same format as CreateGroupByTable
			//'
			ParseGroupByFieldList(FieldList);
			ParseFieldList(GroupBy, true);
			DataRow[] Rows = SourceTable.Select(RowFilter, GroupBy);
			DataRow LastSourceRow = null;
			bool SameRow = false;
			int I = -1;
			int J;
			int K;
			DataRow[] DestRows = new DataRow[m_FieldInfo.Count];
			int[] RowCount = new int[m_FieldInfo.Count];
			//'
			//' Initialize Grand total row
			//'
			DestRows[0] = DestTable.NewRow();
			//'
			//' Process source table rows
			//'
			foreach (DataRow SourceRow in Rows) 
			{
				//'
				//' Determine whether we've hit a control break
				//'
				SameRow = false;
				if (LastSourceRow != null)
				{
					SameRow = true;
					for (I = 0; I < m_FieldInfo.Count; I++) 
					{
						FieldInfo Field = (FieldInfo)(m_FieldInfo[I]);
						if (!ColumnEqual(LastSourceRow[Field.FieldName], SourceRow[Field.FieldName])) 
						{
							SameRow = false;
							break;
						}
					}
					//'
					//' Add previous totals to the destination table
					//'
					if (!SameRow) 
					{
						for (J = m_FieldInfo.Count-1; J >= I + 1; J--) 
						{
							//'
							//' Make NULL the key values for levels that have been rolled up
							//'
							for (K = m_FieldInfo.Count - 1; K >= J; K--) 
							{
								FieldInfo Field = LocateFieldInfoByName(GroupByFieldInfo, ((FieldInfo)m_FieldInfo[K]).FieldName);
								if (Field != null)
								{
									DestRows[J][Field.FieldAlias] = DBNull.Value;
								}
							}
							//'
							//' Make NULL any non-aggregate, non-group-by fields in anything other than
							//' the lowest level.
							//'
							if (J < m_FieldInfo.Count) 
							{
								foreach (FieldInfo Field in GroupByFieldInfo) 
								{
									if (Field.Aggregate != "" || Field.Aggregate != null) 
										break;
									if (LocateFieldInfoByName(m_FieldInfo, Field.FieldName) == null) 
									{
										DestRows[J][Field.FieldAlias] = DBNull.Value;
									}
								}
							}
							//'
							//' Add row
							//'
							DestTable.Rows.Add(DestRows[J]);
						}
					}
				}
				//'
				//' create new destination rows
				//' Value of I comes from previous If block
				//'
				if (!SameRow) 
				{
					for (J = m_FieldInfo.Count-1; J >= I + 1; J--) 
					{
						DestRows[J] = DestTable.NewRow();
						RowCount[J] = 0;
						//if (!Rollup) 
						//{
						//	break;
						//}
					}
				}
				for (J = 0; J < m_FieldInfo.Count; J++) 
				{
					RowCount[J] += 1;
					foreach (FieldInfo Field in GroupByFieldInfo) 
					{
						switch (Field.Aggregate)
						{
							case null:
							case "":
								DestRows[J][Field.FieldAlias] = SourceRow[Field.FieldName];
								break;
							case "last":
								DestRows[J][Field.FieldAlias] = SourceRow[Field.FieldName];
								break;
							case "first":
								if (RowCount[J] == 1)
									DestRows[J][Field.FieldAlias] = 
										SourceRow[Field.FieldName];
								break;
							case "count":
								DestRows[J][Field.FieldAlias] = RowCount[J];
								break;
							case "sum":
								DestRows[J][Field.FieldAlias] = 
									Add(DestRows[J][Field.FieldAlias], 
										SourceRow[Field.FieldName]);
								break;
							case "max":
								DestRows[J][Field.FieldAlias] = 
									Max(DestRows[J][Field.FieldAlias], 
										SourceRow[Field.FieldName]);
								break;
							case "min":
								if (RowCount[J] == 1) 
								{
									DestRows[J][Field.FieldAlias] = SourceRow[Field.FieldName];
								} 
								else 
								{
									DestRows[J][Field.FieldAlias] = Min(DestRows[J][Field.FieldAlias], SourceRow[Field.FieldName]);
								}
								break;
						}
					}
				}
				LastSourceRow = SourceRow;
			}
			if (Rows.Length > 0) 
			{
				//'
				//' Make NULL the key values for levels that have been rolled up
				//'
				for (J = m_FieldInfo.Count -1; J >= 0; J--) 
				{
					for (K = m_FieldInfo.Count -1; K >= J; K--) 
					{
						FieldInfo Field = LocateFieldInfoByName(GroupByFieldInfo, ((FieldInfo)m_FieldInfo[K]).FieldName);
						if (Field != null) 
						{
							DestRows[J][Field.FieldAlias] = DBNull.Value;
						}
					}
					//'
					//' Make NULL any non-aggregate, non-group-by fields in anything other than
					//' the lowest level.
					//'
					if (J != m_FieldInfo.Count -1) 
					{
						foreach (FieldInfo Field in GroupByFieldInfo) 
						{
							if (Field.Aggregate != string.Empty && Field.Aggregate != null) 
								break;
							if (LocateFieldInfoByName(m_FieldInfo, Field.FieldName) == null) 
							{
								DestRows[J][Field.FieldAlias] = DBNull.Value;
							}
						}
					}
					//'
					//' Add row
					//'
					DestTable.Rows.Add(DestRows[J]);
				}
			}
		}
		/// <summary>
		/// Copies the selected rows and columns from SourceTable and inserts them into DestTable
		/// </summary>
		/// <param name="DestTable"></param>
		/// <param name="SourceTable"></param>
		/// <param name="FieldList">Syntax: 
		/// fieldname[ alias]|aggregatefunction(fieldname)[ alias], ...</param>
		/// <param name="RowFilter"></param>
		/// <param name="GroupBy"></param>
		public void InsertGroupByInto(DataTable DestTable, DataTable SourceTable, string FieldList,
			string RowFilter, string GroupBy)
		{
			if (FieldList == null)
				throw new ArgumentException("You must specify at least one field in the field list.");
			ParseGroupByFieldList(FieldList);	//parse field list
			ParseFieldList(GroupBy,false);			//parse field names to Group By into an arraylist
			DataRow[] Rows = SourceTable.Select(RowFilter, GroupBy);
			DataRow LastSourceRow = null, DestRow = null; bool SameRow; int RowCount=0;
			foreach(DataRow SourceRow in Rows)
			{
				SameRow=false;
				if (LastSourceRow!=null)
				{
					SameRow=true;
					foreach(FieldInfo Field in m_FieldInfo)
					{
						if (!ColumnEqual(LastSourceRow[Field.FieldName], SourceRow[Field.FieldName]))
						{
							SameRow=false;
							break;
						}
					}
					if (!SameRow)
						DestTable.Rows.Add(DestRow);
				}
				if (!SameRow)
				{
					DestRow = DestTable.NewRow();
					RowCount=0;
				}
				RowCount+=1;
				foreach(FieldInfo Field in GroupByFieldInfo)
				{
					DataColumn dcSource = SourceRow.Table.Columns[Field.FieldName];
					object objSourceValue;
					if (dcSource == null)
					{
						if (Field.FieldName.ToLower() == "null")
						{
							objSourceValue = DBNull.Value;
						} else if (Field.FieldName.StartsWith("'"))
						{
							objSourceValue = Field.FieldName.Substring(1,Field.FieldName.Length-2).Replace("''", "'");
						} else
						{
							objSourceValue = Field.FieldName;
						}
					} else
					{
						objSourceValue = SourceRow[Field.FieldName];
					}
					switch(Field.Aggregate)    //this test is case-sensitive
					{
						case null:        //implicit last
						case "":        //implicit last
						case "last":
							DestRow[Field.FieldAlias]=objSourceValue;
							break;
						case "first":
							if (RowCount==1)
								DestRow[Field.FieldAlias]=objSourceValue;
							break;
						case "count":
							DestRow[Field.FieldAlias]=RowCount;
							break;
						case "sum":
							DestRow[Field.FieldAlias]=Add(DestRow[Field.FieldAlias], objSourceValue);
							break;
						case "max":
							DestRow[Field.FieldAlias]=Max(DestRow[Field.FieldAlias], objSourceValue);
							break;
						case "min":
							if (RowCount==1)
								DestRow[Field.FieldAlias]=objSourceValue;
							else
								DestRow[Field.FieldAlias]=Min(DestRow[Field.FieldAlias], objSourceValue);
							break;
					}
				}
				LastSourceRow = SourceRow;
			}
			if(DestRow!=null)
				DestTable.Rows.Add(DestRow);
		}
	}
}
