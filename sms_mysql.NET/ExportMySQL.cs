using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

using System.Data;
using System.Data.SqlClient;
using System.Data.OleDb;

using System.Diagnostics;
using System.ServiceProcess;
using System.Threading;

using MySql.Data;
using MySql.Data.MySqlClient;

using System.Globalization;
using Microsoft.SqlServer.Server;

namespace sms_mysql.NET
{
    class ExportMySQL
    {
        OleDbConnection con;
        MySqlConnection mDB;

        int MainID, DebugLevel;
        string logMain;
        string ErrStr, strExt, strTable;
        private int Version, nRows;
        string Shop;
        string skipTables;
        bool Skip;

        private Dictionary<int, string> PosDb;

        public ExportMySQL(string logName)
        {
            logMain = logName;

            Version = 0;
            try
            {
                Shop = Program.GetValue("Shop_Export");
            }
            catch (Exception e)
            {
                Shop = "";
            }
            PosDb = new Dictionary<int, string>();

            skipTables = Program.GetValue("Skip_Tables_Unload").Trim()+",";
        }

        public void log(string str)
        {
            StreamWriter writer;
            try
            {
                Program.CheckRotate(logMain, Program.lSizeLogFile);
                writer = new StreamWriter(logMain, true, System.Text.Encoding.GetEncoding(1251));
            }
            catch
            {
                return;
            }
            writer.WriteLine(String.Format("{0} {1}: {2}", DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString(), str));
            writer.Close();
        }
        private string GetMySqlByObject(object o)
        {
            if (o == null)
                return "NULL";
            if (o == DBNull.Value)
                return "NULL";

            switch (Type.GetTypeCode(o.GetType()))
            {
                case TypeCode.DBNull:
                case TypeCode.Empty:
                    return "NULL";
                case TypeCode.Boolean:
                    return (bool)o ? "true" : "false";
                case TypeCode.Byte:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.SByte:
                    return o.ToString();


                case TypeCode.Char:
                case TypeCode.String:
                case TypeCode.Object:
                    return "'" + MySqlHelper.EscapeString(o.ToString()) + "'";

                case TypeCode.DateTime:
                    return "'" + ((DateTime)o).ToString("yyyy-MM-dd HH:mm:ss") + "'";

                case TypeCode.Decimal:
                    return ((decimal)o).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Double:
                    return ((double)o).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Single:
                    return ((float)o).ToString(CultureInfo.InvariantCulture);
                default:
                    return o.ToString();
            }

        }

        /// <summary>
        /// Creates a multivalue insert for MySQL from a given DataTable
        /// </summary>
        /// <param name="table">reference to the Datatable we're building our String on</param>
        /// <param name="table_name">name of the table the insert is created for</param>
        /// <returns>Multivalue insert String</returns>
        //        public static void BulkInsert(DataRow[] rows, String table_name, MySqlCommand command, int deltaColumn = 0)
        public void BulkInsert(DataRow[] rows, string[] column, String table_name, MySqlCommand command, int deltaColumn = 0)
        {
            int rowNumber = 0;
            int rowCount = rows.Length;
            if (rowCount == 0)
                return;

            int colCount = rows[0].ItemArray.Length - deltaColumn;
            int nCount;

            StringBuilder insBuilder = new StringBuilder();

            insBuilder.AppendFormat("INSERT INTO `{0}` (", table_name);
            nCount = 0;
            foreach (string o in column)
            {
                if (nCount == colCount) break;
                insBuilder.Append(o);
                insBuilder.Append(",");
                nCount++;
            }
            insBuilder[insBuilder.Length - 1] = ')';
            insBuilder.Append(" ");

            //using (var tran = command.Connection.BeginTransaction())
            //{
            StringBuilder queryBuilder = new StringBuilder();
            while (rowNumber < rowCount)
            {
                DataRow row = rows[rowNumber];

                if (table_name.ToUpper() == "ITEMS")
                {
//                    log(String.Format("{0,6:D6} {1,9:F2} {2,9:F2}  {3}",(decimal)row[27],row[26],row[0],row[1]));
                    log(String.Format(" {0,6:D6} {1,9:F2} {2,9:F2}  {3}", row[0], (decimal)row[27], row[26], row[1]));
                }

                if (queryBuilder.Length == 0)
                {
                    // queryBuilder.AppendFormat("INSERT INTO `{0}` VALUES ", table_name);
                    queryBuilder.Append(insBuilder.ToString());
                    queryBuilder.Append(" VALUES");
                }
                else
                    queryBuilder.Append("\n,");
                queryBuilder.Append("(");
                nCount = 0;
                foreach (object o in row.ItemArray)
                {
                    if (nCount == colCount) break;
                    if ((column[nCount].ToUpper() == "PASSWORD") && (table_name.ToUpper() == "USERS"))
                        queryBuilder.Append("old_password(");
                    
                    queryBuilder.Append(GetMySqlByObject(o));
       
                    if ((column[nCount].ToUpper() == "PASSWORD") && (table_name.ToUpper() == "USERS"))
                        queryBuilder.Append(")");
                    queryBuilder.Append(",");
                    nCount++;
                }
                queryBuilder[queryBuilder.Length - 1] = ')';

                if (queryBuilder.Length > 320000)
                {
                    command.CommandText = queryBuilder.ToString();
                    //SqlContext.Pipe.Send(rowNumber.ToString() + "  " + queryBuilder.Length + "   " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                    command.ExecuteNonQuery();
                    queryBuilder = new StringBuilder();
                }
                rowNumber++;
            }
            if (queryBuilder.Length > 0)
            {
                command.CommandText = queryBuilder.ToString();
                //SqlContext.Pipe.Send(rowNumber.ToString() + "  " + queryBuilder.Length + "   " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                command.ExecuteNonQuery();
            }
            //    tran.Commit();
            //}
        }


        public void GetLine()
        {
            OleDbDataReader reader;

            reader = ExecuteReader(con, "select DISTINCT l.* from line l inner join CashLines cs ON cs.LineID=l.LineID"); ;
            if (reader == null)
            {
                log(String.Format("Ошибка при чтени линеек касс : {0}", ErrStr));
                return;
            }

            PosDb.Clear();

            while (reader.Read())
            {
                PosDb.Add((int)reader["LineID"], reader["ExportDB"].ToString());
                if (DebugLevel > 0)
                    log(string.Format("Линейка {0} БД {1}", reader["LineID"], reader["ExportDB"]));
                //ImportDataLine(reader["Name"].ToString(), reader["ImportDB"].ToString());
            }
            reader.Close();
        }

        bool Open()
        {
            OleDbConnectionStringBuilder sqlBulder = new OleDbConnectionStringBuilder();
            sqlBulder["Persist Security Info"] = "True";
            sqlBulder["Provider"] = "SQLOLEDB.1";
            sqlBulder["Data Source"] = Program.cfg["SMS_Server"];
            sqlBulder["User Id"] = Program.cfg["SMS_User"];
            sqlBulder["Password"] = Program.cfg["SMS_Passwd"];
            sqlBulder["Initial Catalog"] = Program.cfg["SMS_Baza"];
//            sqlBulder["Connection Lifetime"] = "0";

            con = new OleDbConnection(sqlBulder.ConnectionString);
            try
            {
                con.Open();
            }
            catch (Exception e)
            {
                log(String.Format("Ошибка при открытии SMS: {0}", e.ToString()));
                //Program.killAll = true;
                return false;
            }

            MySqlConnectionStringBuilder mb = new MySqlConnectionStringBuilder();
            mb.Server = Program.GetValue("MySQL_Server");
            mb.UserID = Program.GetValue("MySQL_User");
            mb.Password = Program.GetValue("MySQL_Passwd");
            mb.CharacterSet = "utf8";

            mDB = new MySqlConnection(mb.ConnectionString);
            try
            {
                mDB.Open();
            }
            catch (Exception e)
            {
                if (con.State != ConnectionState.Closed)
                {
                    con.Close();
                }

                log(String.Format("Ошибка при подключении к MySQL: {0}", e.ToString()));
                //Program.killAll = true;
                return false;
            }

            OleDbDataReader reader = ExecuteReader(con, "SELECT ID FROM i_Managers where Name='Супермаг УКМ4'");
            if (reader == null)
            {
                log(String.Format("Ошибка при чтении i_Managers : {0}", ErrStr));
                Close();
                return false;
            }
            while (reader.Read())
            {
                MainID = (int)reader[0];
                break;
            }
            reader.Close();

            DebugLevel = 0;
            GetLine();

            return true;
        }

        void Close()
        {
            if (con.State != ConnectionState.Closed)
            {
                con.Close();
            }
            if (mDB.State != ConnectionState.Closed)
            {
                mDB.Close();
            }
        }

        public OleDbDataReader ExecuteReader(OleDbConnection con, string Query)
        {
            OleDbDataReader reader;
            OleDbCommand cmd = con.CreateCommand();
            cmd.CommandTimeout = 1200;

            try
            {
                cmd.CommandText = Query;
                reader = cmd.ExecuteReader();
                ErrStr = "";
            }
            catch (Exception e)
            {
                nRows = 0;
                ErrStr = e.Message.Trim();

                return null;
            }

            return reader;
        }

        public bool ExecuteScalar(OleDbConnection con, string Query, out object res, out string ErrStr)
        {
            OleDbCommand cmd = con.CreateCommand();
            cmd.CommandTimeout = 1200;
            try
            {
                cmd.CommandText = Query;
                res = cmd.ExecuteScalar();
                if (res == null) res = (int)0;
                ErrStr = "";
            }
            catch (Exception e)
            {
                res = 0;
                ErrStr = e.Message.Trim();

                return false;
            }

            return true;
        }

        bool ExecuteNonQuery(OleDbConnection con, string Query)
        {
            OleDbCommand cmd = con.CreateCommand();
            cmd.CommandTimeout = 1200;
            try
            {
                cmd.CommandText = Query;
                nRows = cmd.ExecuteNonQuery();
                ErrStr = "";
            }
            catch (Exception e)
            {
                nRows = 0;
                ErrStr = e.Message.Trim();

                return false;
            }

            return true;
        }

        public bool ExecuteScalarMySql(string Query, out object res, out string ErrStr)
        {
            MySqlCommand cmd = mDB.CreateCommand();
            try
            {
                cmd.CommandText = Query;
                cmd.CommandTimeout = Program.commandMySQLTimeout;
                res = cmd.ExecuteScalar();
                if (res == null) res = (int)0;
                ErrStr = "";
            }
            catch (Exception e)
            {
                res = 0;
                ErrStr = e.Message.Trim();

                return false;
            }

            return true;
        }

        bool ExecuteNonQueryMySql( string Query)
        {
            MySqlCommand cmd = mDB.CreateCommand();
            try
            {
                cmd.CommandText = Query;
                cmd.CommandTimeout = Program.commandMySQLTimeout;
                nRows = cmd.ExecuteNonQuery();
                ErrStr = "";
            }
            catch (Exception e)
            {
                nRows = 0;
                ErrStr = e.Message.Trim();

                return false;
            }

            return true;
        }

        private bool SkipTable(OleDbDataReader reader)
        {
            log(String.Format("{0} Пропускаем", strTable));
            while (reader.Read())
            {
                string strField0 = reader.GetName(0);
                //string strData = reader.GetString(0);

            }

            return true;
        }

        private bool SqlToMySQL(OleDbDataReader reader, String table_name)
        {
            DataTable dataColumn = new DataTable();
            DataTable dataTable = new DataTable();
            MySqlCommand command = mDB.CreateCommand();
            string ErrStr;
            object id;

            try
            {
                if (!ExecuteScalarMySql(String.Format("DELETE FROM {0};", table_name), out id, out ErrStr))
                {
                    nRows = -1;
                    log(String.Format("Ошибка при чистке таблицы {1} = {0}", ErrStr, table_name));
                    return false;
                }

                //dataTable.Load(reader);

                //DataTable dt = reader.GetSchemaTable();
                int c = reader.FieldCount;//dt.Rows.Count;
                string[] mas = new string[c];

                for (int i = 0; i <= c - 1; i++)
                {
                    mas[i] = reader.GetName(i);//dt.Rows[i][0].ToString();
                }
                DataTable dt = reader.GetSchemaTable();
                foreach (DataRow row in dt.Rows)
                {
                    string colName = row.Field<string>("ColumnName");
                    Type t = row.Field<Type>("DataType");
                    dataTable.Columns.Add(colName, t);
                }
                while (reader.Read())
                {
                    var newRow = dataTable.Rows.Add();
                    foreach (DataColumn col in dataTable.Columns)
                    {
                        newRow[col.ColumnName] = reader[col.ColumnName];
                    }
                }
                int d = 0;
                if (String.Compare(table_name, "items", true) == 0) d = 2;
                BulkInsert(dataTable.Select(), mas, table_name, command, d);
                log(String.Format("Добавили в {0} {1} строк", table_name,dataTable.Rows.Count));
            }
            catch (Exception e)
            {
                ErrStr = e.Message.Trim();
                //Console.WriteLine(ErrStr);
                log(String.Format("Ошибка при записи в таблицу {0} : {1}",table_name, ErrStr));
                return false;
            }

            return true;
        }

        private bool CheckSignal()
        {
            object id;
            if (!ExecuteScalarMySql( "select count(*) c from `signal`;", out id, out ErrStr))
            {
                nRows = -1;
                log(String.Format("Ошибка при чтении таблицы `signal` = {0}", ErrStr));
                return false;
            }

            nRows = int.Parse(id.ToString());//(int)id;
            return true;
        }

        private bool UpLoad(OleDbDataReader reader)
        {
            strExt = "";
            int cashID = -1;
            int CashBusy = -1;
            int state = -1;
            string strUpdate = "";
            //object obj;
           // DataTable dataTable = new DataTable();

            int LineID = -1;

            do
            {
                switch (state)
                {
                    case -1:
                        if (!reader.Read()) break;
                        if ((reader.GetName(0) == "TABLE") && (reader[0].ToString() == "HEADER"))
                        {
                            state = 0;
                            cashID = -1; CashBusy = 0;
                            //dataTable.Load(reader);
                            //strTable = "";
                        }
                        break;
                    case 0:
                        if (!reader.Read()) break;
                        if (reader.GetName(0).ToUpper() == "CASHID")
                        {
                            if (LineID != -1)
                            {
                                if (CashBusy == 0)
                                {
                                    string strTemp;
                                    strTemp = String.Format(" update #cashman set Processing=2 " +
                                                                           "WHERE CashID IN (SELECT CashID FROM CashLines WHERE lineid={0})", cashID);
                                    if (ExecuteNonQueryMySql( "INSERT INTO `signal` (`signal`,`version`) VALUES ('" + strExt.Trim() + "',0);"))
                                    {
                                        strUpdate += strTemp;
                                    }
                                    else
                                        log(String.Format("Ошибка при добавлении в `signal` : {0}", ErrStr));
                                }
                            }

                            state = 1;
                            cashID = (int)reader[0];
                            LineID = cashID;
                            strExt = reader[1].ToString();
                            string posKey = String.Format("Pos_{0}", cashID);
                            if (!PosDb.ContainsKey(cashID))
                            {
                                log(String.Format("Кассовая линейка {0} ({1}) не прописана в конфиге", cashID, posKey));
                                CashBusy = 1;
                            }
                            else
                            {

                                log(String.Format("Обработка кассовой линейки {0}", cashID));
                                mDB.ChangeDatabase(PosDb[cashID]);

                                if (!CheckSignal())
                                {
                                    log("*ОШИБКА* select * from signal : " + ErrStr);
                                    CashBusy = 1;
                                    break;
                                }
                                if (nRows != 0)
                                {
                                    log(String.Format("Линейка {0} занята...", cashID));
                                    CashBusy = 1;
                                    break;
                                }
                                CashBusy = 0;
                            }
                        }
                        //dataTable.Load(reader);
                        break;

                    case 1:
                        if (!reader.Read()) break;
                        if ((reader.GetName(0) == "TABLE") && (reader[0].ToString() == "HEADER"))
                        {
                            if (CashBusy == 0)
                            {
                                strUpdate += String.Format("update #cashman set Processing=2 " +
                                                                   "WHERE CashID IN (SELECT CashID FROM CashLines WHERE lineid={0})", cashID);
                            }

                            state = 0;
                            cashID = -1; CashBusy = 0;
                            strTable = "";
                            break;
                        }

                        Skip = false;
                        if (CashBusy == 1) break;
                        if (reader.GetName(0).ToUpper() == "TABLE")
                        {
                            strTable = reader[0].ToString();
                            state = 2;
                            if (skipTables.IndexOf(strTable.Trim() + ",") != -1) Skip = true;
                        }
                        //dataTable.Load(reader);
                        break;

                    case 2:
                        if ((CashBusy == 0) && !Skip)
                        {
//                            if (!CopyToTable(cMySql, reader))
                            if (!SqlToMySQL(reader, strTable))
                            {
                                reader.Close();

                                return false;
                            }
                        }
                        else
                            //dataTable.Load(reader);
                            SkipTable(reader);

                        //if ((nRows == 0) && (strExt == "upd"))
                        //    File.Delete(Path.Combine(DirectoryDbf.Trim(), strTable.Trim() + ".DBF"));

                        state = 1;
                        break;
                }

            } while (reader.NextResult());

            reader.Close();

            if (LineID != -1)
            {
                if (CashBusy == 0)
                {
                    string strTemp;
                    strTemp = String.Format(" update #cashman set Processing=2 " +
                                                           "WHERE CashID IN (SELECT CashID FROM CashLines WHERE lineid={0})", cashID);
                    if ( ExecuteNonQueryMySql( "INSERT INTO `signal` (`signal`,`version`) VALUES ('" + strExt.Trim() + "',0);") )
                    {
                        strUpdate += strTemp;
                    }
                    else
                        log(String.Format("Ошибка при добавлении в `signal` : {0}", ErrStr));
                }
            }

            if (strUpdate != "")
            {
                ExecuteNonQuery(con, strUpdate);
                if (ErrStr != "")
                    log("*ОШИБКА* update #cashman set Processing=2 : " + ErrStr);
            }

            return true;
        }

        private bool UpLoadAllCash()
        {
//            object obj;
            string PriceListID = Program.GetValue("PriceList_ID");

            if (!ExecuteNonQuery(con, "update #cashman set Processing=1"))
            {
                log(String.Format("Ошибка при выполнении \'update #cashman set Processing=1\'= {0}", ErrStr));
                return false;
            }

            if (!ExecuteNonQuery(con, "i_sm_Prepare"))
            {
                log(String.Format("Ошибка при выполнении \'i_sm_Prepare\'= {0}", ErrStr));
                return false;
            }

            OleDbDataReader reader = ExecuteReader(con, String.Format("exec i_sm_UKM '{0}', {1}, {2}", Shop.Trim(), Version, PriceListID));
            if (reader == null)
            {
                log(String.Format("Ошибка при выполнении \'i_sm_UKM\'= {0}", ErrStr));
                return false;
            }

            if (!UpLoad(reader)) return false;

            if (!ExecuteNonQuery(con, "i_sm_Commit"))
            {
                log(String.Format("Ошибка при выполнении \'i_sm_Commit\'= {0}", ErrStr));
                return false;
            }

            log("================  Выгрузка на кассовые линейки завершена ================");

            return true;
        }

        private bool CheckData()
        {
            string szFmtStart =  Properties.Resources.ResourceManager.GetString("ExportFmtStart");
            OleDbDataReader reader;

            string Query = String.Format(szFmtStart, MainID);
            reader = ExecuteReader(con, Query);
            if (reader == null)
            {
                log(Query);
                log(String.Format("Ошибка при проверке наличия данных на выгрузку : {0}", ErrStr));
                return false;
            }
            while (reader.Read())
            {
                reader.Close();
                return true;
            }
            reader.Close();

            return false;
        }

        public void Export()
        {
            try
            {
                if (!Open()) return;

                if (CheckData()) UpLoadAllCash();

                Close();
            }
            catch (Exception e)
            {
                ErrStr = e.Message.Trim();
                log(String.Format("Ошибка при выгрузке : {0}", ErrStr));
            }
        }
    }
}
