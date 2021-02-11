using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.IO;
using System.Reflection;

using System.Data;
using System.Data.SqlClient;
using System.Data.OleDb;

using System.Diagnostics;
using System.ServiceProcess;
using System.Threading;

using MySql.Data;
using MySql.Data.MySqlClient;

using System.Globalization;
//using Microsoft.SqlServer.Server;

namespace sms_mysql.NET
{
    class CLineImport
    {
        public int      LineID;
        public string   Line;
        public string   ImportDb;
    }

    class ImportMySQL
    {
        SqlConnection con;
        MySqlConnection mDB;

        string logMain, ErrStr;
        int nRows, nChecks, nShift;
        int DebugLevel;
        int timeOut;

        List<CLineImport> vLine;
        private Dictionary<string, string> tmpTable;

        string ukm_Verion, fullUkmVersion, ukm_dateUpdate;

        string workDB;
        DataTable indexTable;

        public ImportMySQL(string logName)
        {
            logMain = logName;

            //Microsoft.SqlServer.Server.

            timeOut = Convert.ToInt32(Program.GetValue("CommandTimeOut", "1000"));

            tmpTable = new Dictionary<string, string>();
            tmpTable["#i_Shift"] = "SELECT * INTO #i_Shift FROM i_UKM_SHIFT WHERE 1=0";
            tmpTable["#i_CashPay"] = "SELECT * INTO #i_CashPay FROM i_UKM_CASHPAY WHERE 1=0";
            tmpTable["#i_CashSail"] = "SELECT * INTO #i_CashSail FROM i_UKM_CASHSAIL WHERE 1=0";
            tmpTable["#i_CashDisc"] = "SELECT * INTO #i_CashDisc FROM i_UKM_CASHDISC WHERE 1=0";
            tmpTable["#i_CashDcrd"] = "SELECT * INTO #i_CashDcrd FROM i_UKM_CASHDCRD WHERE 1=0";
            tmpTable["#i_CashAuth"] = "SELECT * INTO #i_CashAuth FROM i_UKM_CASHAUTH WHERE 1=0";
            tmpTable["#i_CashPayDiscount"] = "SELECT * INTO #i_CashPayDiscount FROM i_UKM_CASHPAY_DISCOUNT WHERE 1=0";
            tmpTable["#i_CashSailDiscount"] = "SELECT * INTO #i_CashSailDiscount FROM i_UKM_CASHSAIL_DISCOUNT WHERE 1=0";
            tmpTable["#trm_out_receipt_egais"] = "SELECT * INTO #trm_out_receipt_egais FROM trm_out_receipt_egais WHERE 1=0";
        }

        public MySqlDataReader ExecuteReaderMySql(string Query)
        {
            MySqlDataReader reader;
            MySqlCommand cmd = mDB.CreateCommand();

            try
            {
                cmd.CommandTimeout = timeOut;
                cmd.CommandText = Query;
                cmd.CommandTimeout = Program.commandMySQLTimeout;
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

        public bool ExecuteScalarMySql(string Query, out object res, out string ErrStr)
        {
            MySqlCommand cmd = mDB.CreateCommand();
            try
            {
                cmd.CommandTimeout = timeOut;
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

        bool ExecuteNonQueryMySql(string Query)
        {
            MySqlCommand cmd = mDB.CreateCommand();
            try
            {
                cmd.CommandTimeout = timeOut;
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

        bool TableExistMySql(string TableName)
        {
            if (!ExecuteNonQueryMySql(String.Format("show tables like '{0}';", TableName.Trim())))
            {
                return false;
            }
            if (nRows <= 0) return false;

            return true;
        }

        bool Open()
        {
            SqlConnectionStringBuilder sqlBulder = new SqlConnectionStringBuilder();
            sqlBulder["Persist Security Info"] = "True";
            sqlBulder["Data Source"] = Program.cfg["SMS_Server"];
            sqlBulder["User Id"] = Program.cfg["SMS_User"];
            //sqlBulder["Password"] = "1551";
            sqlBulder["Password"] = Program.cfg["SMS_Passwd"];
            sqlBulder["Initial Catalog"] = Program.cfg["SMS_Baza"];
            sqlBulder["Connection Lifetime"] = "0";

            con = new SqlConnection(sqlBulder.ConnectionString);
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
            indexTable = new DataTable();

            indexTable.Columns.Add("TableName");
            indexTable.Columns.Add("IndexName");

            GetLine();
            return LoadIndex();

            //return true;
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

        void CheckIndex(string strTable, string strIndexName, string strIndexBody)
        {
            string filter = String.Format("TableName='{0}.{1}' AND IndexName='{2}'",workDB.Trim(), strTable.Trim(), strIndexName.Trim());
            DataRow[] rd = indexTable.Select(filter);
            if (rd.Length == 0)
            {
                ExecuteNonQueryMySql(strIndexBody);
            }
        }

        void CheckIndexSMS(string strTable, string strIndexName, string strIndexBody)
        {
            Object obj;
            string ErrStr;

            if (!Program.ExecuteSqlNonQuery(con, strIndexBody, out obj, out ErrStr))
            {
                //log(String.Format("Ошибка при создании IndexName '{2}' для '{0}' = {1}", strTable, ErrStr, strIndexName.Trim()));
                return;
            }
        }

        Boolean LoadIndex()
        {
            List<string> list = new List<string>();

            MySqlDataReader rd;//= ExecuteReaderMySql(string.Format("SHOW TABLES FROM"));
            foreach (CLineImport v in vLine)
            {
                list.Clear();

                rd= ExecuteReaderMySql(string.Format("SHOW TABLES FROM {0};",v.ImportDb.Trim()));
                while (rd.Read())
                {
                    list.Add(rd[0].ToString());
                }
                rd.Close();

                foreach (string t in list)
                {
                    //log(String.Format("SHOW INDEX FROM {0}.{1};", v.ImportDb.Trim(), t.Trim()));
                    try
                    {
                        rd = ExecuteReaderMySql(String.Format("SHOW INDEX FROM {0}.{1};", v.ImportDb.Trim(), t.Trim()));
                    }
                    catch (Exception e)
                    {
                        //ErrStr = e.Message.Trim();
                        //Console.WriteLine(ErrStr);
                        log(String.Format("Ошибка при чтении индексов таблицы {0} - {1}", t.Trim(), e.Message.Trim()));
                        return false;
                    }
                    if (rd == null)
                    {
                        log(String.Format("***** Ошибка при чтении индексов таблицы {2}.{0} - {1}", t.Trim(), ErrStr.Trim(), v.ImportDb.Trim()));
                        return false;
                    }
                    while (rd.Read())
                    {
                        //                        indexTable.Rows.Add(t.Trim(), rd[2].ToString());
                        indexTable.Rows.Add(String.Format("{0}.{1}", v.ImportDb.Trim(), t.Trim()), rd[2].ToString());
                    }
                    rd.Close();
                }
            }

            return true;
//            int k = 0;
        }

        void GetLine()
        {
            SqlDataReader rd = Program.ExecuteSqlReader(con, "select DISTINCT l.* from line l (NOLOCK) inner join CashLines cs (NOLOCK) ON cs.LineID=l.LineID", out ErrStr);
            vLine = new List<CLineImport>();
            if (rd == null)
            {
                return;
            }

            while (rd.Read())
            {
                CLineImport l = new CLineImport();
                l.LineID = (int)rd["LineID"];
                l.Line = rd["Name"].ToString();
                l.ImportDb = rd["ImportDB"].ToString();

                vLine.Add(l);
            }
            rd.Close();
        }

        void GetUkmVersion()
        {
            MySqlDataReader rd = ExecuteReaderMySql("SELECT substring(trim(cast(version AS CHAR(10))), 1, 2) s,trim(cast(version AS CHAR(10))) f, moment FROM ukmserver.ukm_ukmversion uk ORDER BY moment DESC LIMIT 1;");
            while (rd.Read())
            {
                ukm_Verion = rd["s"].ToString();
                fullUkmVersion = rd["f"].ToString();
                ukm_dateUpdate = rd["moment"].ToString();
                break;
            }
            rd.Close();
        }

        bool DeleteTemporaryTable()
        {
            object obj;
            if (!Program.ExecuteSqlNonQuery(con,Properties.Resources.DeleteTemporaryTable, out obj, out ErrStr))
            {
                log(String.Format("DELETE Temporary Tables в БД: {0}", ErrStr));
                return false;
            }

            return true;
        }

        bool CreateTemporaryTable()
        {
            object obj;
            if (!Program.ExecuteSqlNonQuery(con, 
                "if object_id('Check_EGAIS') is null    \r\n" +
                "   CREATE TABLE [Check_EGAIS](   \r\n" +
                "   	CeIDD		bigint IDENTITY(1,1) NOT NULL,   \r\n" +
                "   	ShiftID		int,   \r\n" +
                "   	CheeckID	int,   \r\n" +
                "   	Url			varchar(4096),   \r\n" +
                "   	primary key clustered (CeIDD)   \r\n" +
                ")"
                , out obj, out ErrStr))
            {
                log(String.Format("Ошибка при проверке существования таблицы 'Check_EGAIS': {0}", ErrStr));
                return false;
            }
            if (!Program.ExecuteSqlNonQuery(con,
                "if object_id('Check_Order_Order') is null    \r\n" +
                "   CREATE TABLE [Check_Order_Order](   \r\n" +
                "   	CoIDD		bigint IDENTITY(1,1) NOT NULL,   \r\n" +
                "   	ShiftID		int,   \r\n" +
                "   	CheckID	    int,   \r\n" +
                "   	BarCode	    varchar(80),   \r\n" +
                "       ZakazNo     bigint,   \r\n" +       // Номер заказа ИМ в мягком чеке
                "   	primary key clustered (CoIDD)   \r\n" +
                ")"
                , out obj, out ErrStr))
            {
                log(String.Format("Ошибка при проверке существования таблицы 'Check_EGAIS': {0}", ErrStr));
                return false;
            }


            if (!Program.ExecuteSqlNonQuery(con,
                "if object_id('trm_out_receipt_egais') is null    \r\n" +
                "   CREATE TABLE [trm_out_receipt_egais](   \r\n" +
                "   	[cash_id] int NOT NULL,   \r\n" +
                "   	[id] int NOT NULL,   \r\n" +
                "   	[url] varchar(4096) NULL,   \r\n" +
                "   	[sign] varchar(512) NULL,   \r\n" +
                "   	[version] int NOT NULL,   \r\n" +
                "   	[deleted] int NOT NULL  \r\n" +
                ")"
                , out obj, out ErrStr))
            {
                log(String.Format("Ошибка при проверке существования таблицы 'trm_out_receipt_egais': {0}", ErrStr));
                return false;
            }

            if (!Program.ExecuteSqlNonQuery(con, Properties.Resources.CreateTemporaryTable, out obj, out ErrStr))
            {
                log(String.Format("CREATE Temporary Tables в БД: {0}", ErrStr));
                return false;
            }

            foreach (KeyValuePair<string, string> entry in tmpTable)
            {
                if (!Program.ExecuteSqlNonQuery(con, entry.Value, out obj, out ErrStr))
                {
                    log(String.Format("CREATE Temporary Tables {1} в БД: {0}", ErrStr, entry.Key));
                    return false;
                }
            }

            return true;
        }

        string GetTypeString(Type dc,int len, int dec)
        {
            //string res="";
            StringBuilder sql = new StringBuilder();
            bool isNumeric = false;
            if (len > 8000) len = 8000;

            switch (dc.ToString().ToUpper())
            {
                case "SYSTEM.INT16":
                    sql.Append(" smallint");
                    isNumeric = true;
                    break;
                case "SYSTEM.INT32":
                    sql.Append(" int");
                    isNumeric = true;
                    break;
                case "SYSTEM.UINT32":
                case "SYSTEM.INT64":
                case "SYSTEM.UINT64":
                    sql.Append(" bigint");
                    isNumeric = true;
                    break;
                case "SYSTEM.DATETIME":
                    sql.Append(" datetime");
//                    usesColumnDefault = false;
                    break;
                case "SYSTEM.STRING":
                    sql.AppendFormat(" varchar({0})", len);
                    break;
                case "SYSTEM.SINGLE":
                    sql.Append(" single");
                    isNumeric = true;
                    break;
                case "SYSTEM.DOUBLE":
                    sql.Append(" double");
                    isNumeric = true;
                    break;
                case "SYSTEM.DECIMAL":
                    sql.AppendFormat(" decimal({0}, {1})", len, dec);
                    isNumeric = true;
                    break;
                case "SYSTEM.SBYTE":
                    sql.Append(" tinyint");
                    isNumeric = true;
                    break;
                default:
                    sql.AppendFormat(" varchar({0})", len);
                    break;
            }

            return sql.ToString();
        }

        string GetScriptCreateTable(DataTable dt, string TableName)
        {
            StringBuilder Query = new StringBuilder("");
            List<DataColumn> cList = new List<DataColumn>();

            foreach (DataColumn column in dt.Columns)
            {
                cList.Add(column);
            }

            DataColumn[] Columns = cList.ToArray();

            Query.Append(String.Format("CREATE TABLE {0} ( \r\n", TableName));

            int i = 0;
            foreach (DataRow row in dt.Rows)
            {
                Query.Append("      ");
                if (i > 0) Query.Append(",[");
                else Query.Append("[");
                Query.Append(row[0]);
                Query.Append("]      ");
                Query.Append(GetTypeString((Type)row[11], (int)row[2], (int)row[4]));
                Query.Append("\r\n");

                i++;
            }

            //for (int i = 0; i < Columns.Length; i++)
            //{
            //    Query.Append("      ");
            //    if (i > 0) Query.Append(",");
            //    Query.Append(dt.Columns[i].ColumnName);
            //    Query.Append("      ");
            //    Query.Append(GetTypeString(dt.Columns[i]));
            //    Query.Append("\r\n");
            //}

            Query.Append(" )");

            return Query.ToString();
        }

        bool DeleteTable(string TableName)
        {
            StringBuilder q = new StringBuilder();

            q.Append("if object_id(");
            if (TableName.Substring(0, 1) == "#") q.Append("'tempdb..");
            else q.Append("'");
            q.Append(TableName.Trim());
            q.Append("') is not null\r\n    DROP TABLE ");
            q.AppendLine(TableName.Trim());
            object obj;
            if (!Program.ExecuteSqlNonQuery(con, q.ToString(), out obj, out ErrStr))
            {
                log(String.Format("Ошибка при удалении {0} = {1}", TableName, ErrStr));
                return false;
            }
            return true;
        }

        bool CopyMySqlToSms(string TableName, string QueryMySQL)
        {
            string Query;
            MySqlDataReader rdMySQL = ExecuteReaderMySql(QueryMySQL);

            if (!DeleteTable(TableName)) return false;

            if (TableName != "#u_receipt_discounts_")
            {
                DataTable dtSchema = rdMySQL.GetSchemaTable();

                Query = GetScriptCreateTable(dtSchema, TableName);
            }
            else
            {
                Query =
                    "CREATE TABLE #u_receipt_discounts (  \r\n" +
                    "  store varchar(100),	\r\n" +
                    "  cash_number bigint ,	\r\n" +
                    "  cash_id bigint ,	\r\n" +
                    "  id bigint ,	\r\n" +
                    "  receipt_header bigint ,	\r\n" +
                    "  name varchar(100),	\r\n" +
                    "  type int ,	\r\n" +
                    "  discount_type int,	\r\n" +
                    "  card_type bigint,	\r\n" +
                    "  card_number varchar(40),	\r\n" +
                    "  marketing_effort_id bigint ,	\r\n" +
                    "  marketing_effort_name varchar(100),	\r\n" +
                    "  advertising_campaign_id bigint ,	\r\n" +
                    "  advertising_campaign_name varchar(100)	\r\n" +
                    ")";
            }

            object obj;
            if (!Program.ExecuteSqlNonQuery(con, Query, out obj, out ErrStr))
            {
                rdMySQL.Close();
                log(String.Format("Ошибка при создании {0} = {1}",TableName,ErrStr));
                return false;
            }

            SqlBulkCopy bulkData = new SqlBulkCopy(con);
            bulkData.DestinationTableName = TableName;
            try
            {

                bulkData.WriteToServer(rdMySQL);
            }
            catch (SqlException ex)
            {
                if (ex.Message.Contains("Received an invalid column length from the bcp client for colid"))
                {
                    string pattern = @"\d+";
                    Match match = Regex.Match(ex.Message.ToString(), pattern);
                    var index = Convert.ToInt32(match.Value) - 1;

                    FieldInfo fi = typeof(SqlBulkCopy).GetField("_sortedColumnMappings", BindingFlags.NonPublic | BindingFlags.Instance);
                    var sortedColumns = fi.GetValue(bulkData);
                    var items = (Object[])sortedColumns.GetType().GetField("_items", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(sortedColumns);

                    FieldInfo itemdata = items[index].GetType().GetField("_metadata", BindingFlags.NonPublic | BindingFlags.Instance);
                    var metadata = itemdata.GetValue(items[index]);

                    var column = metadata.GetType().GetField("column", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).GetValue(metadata);
                    var length = metadata.GetType().GetField("length", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).GetValue(metadata);
                    throw new System.FormatException(String.Format("Column: {0} contains data with a length greater than: {1}", column, length));
                }

                throw;
            } 
            bulkData.Close();

            rdMySQL.Close();

            SqlCommand countRow = new SqlCommand(
                String.Format("SELECT COUNT(*) FROM {0};", TableName),
                con);

            nRows = System.Convert.ToInt32(
                    countRow.ExecuteScalar());

            return true;
        }

        // Получить Расхождения Сумм
        SqlDataReader GetDifferencesSum(SqlConnection cn)
        {

            SqlDataReader rd = Program.ExecuteSqlReader(con,
            "SELECT											\r\n" +
            "   CASHNUMBER,CHECKNUMBER,ZNUMBER,             \r\n" +
            "	ROUND(SUM(TOTALRUB),2) as TOTALRUB,			\r\n" +
            "	ROUND(SUM(PAYEDRUB),2) as PAYEDRUB			\r\n" +
            "FROM											\r\n" +
            "(												\r\n" +
            "	SELECT										\r\n" +
            "       CASHNUMBER,CHECKNUMBER,ZNUMBER,         \r\n" +
            "		ROUND(SUM(TOTALRUB),2) as TOTALRUB,		\r\n" +
            "		0 as PAYEDRUB							\r\n" +
            "	FROM #i_CASHSAIL							\r\n" +
            "	GROUP BY                                    \r\n" +
            "		CASHNUMBER,CHECKNUMBER,ZNUMBER			\r\n" +
            "												\r\n" +
            "	UNION ALL									\r\n" +
            "												\r\n" +
            "	SELECT										\r\n" +
            "       CASHNUMBER,CHECKNUMBER,ZNUMBER,         \r\n" +
            "		0 as TOTALRUB,							\r\n" +
            "		ROUND(SUM(PAYEDRUB),2) as PAYEDRUB		\r\n" +
            "	FROM #i_CASHPAY								\r\n" +
            "	GROUP BY                                    \r\n" +
            "		CASHNUMBER,CHECKNUMBER,ZNUMBER			\r\n" +
            ") Z											\r\n" +
            "GROUP BY                                       \r\n" +
            "	CASHNUMBER,CHECKNUMBER,ZNUMBER				\r\n" +	
            "HAVING 	                                    \r\n" +
            "	ROUND(SUM(TOTALRUB),2) <> ROUND(SUM(PAYEDRUB),2)\r\n"
                , out ErrStr
                );
            if (rd == null)
            {
                log(String.Format("Ошибка при чтении Расхождения Сумм по чекам {0}", ErrStr));
                return null;
            }

            return rd;
        }

        struct RecItem {
            public Int32 cash_id;
            public UInt64 id;
            public string Tovar;
            public decimal amount;
        };

        void ChangeSummChecks(string CashID, string CheckID)
        {
            string Query, Err_Str="";
            object obj;
            Decimal amount, amountReceipt, Delta;
            RecItem[] vRecItem = new RecItem[300];
            int iRecItem = 0, item = 0;
            decimal znak = 1;
            MySqlDataReader rdMySQL;


            Query = String.Format(
                "SELECT  SUM(rp.real_amount) amount FROM receipt r \r\n" +
                "INNER JOIN receipt_item rp ON rp.receipt_header=r.id AND rp.cash_id=r.cash_id \r\n" +
                "WHERE r.cash_id={0} AND r.id={1} AND rp.type=0"
                , CashID, CheckID);
            if (!ExecuteScalarMySql(Query, out obj, out ErrStr))
            {
                log(String.Format("ChangeSummChecks: Ошибка при чтении итоговых сумм {0}", Err_Str));
                return;
            }
            amountReceipt = (Decimal)obj;

            Query = String.Format(
                "SELECT SUM(rp.amount) amount FROM receipt r \r\n" +
                "INNER JOIN receipt_payment rp ON rp.receipt_header=r.id AND rp.cash_id=r.cash_id \r\n" +
                "WHERE r.cash_id={0} AND r.id={1} AND rp.type IN (0,1,4)"
                , CashID, CheckID);
            if (!ExecuteScalarMySql(Query, out obj, out ErrStr))
            {
                log(String.Format("ChangeSummChecks: Ошибка при чтении итоговых сумм {0}", Err_Str));
                return;
            }

            amount = (Decimal)obj;

            Delta = amountReceipt - amount;
            if (Delta < (decimal)0)
            {
                Delta = -Delta;
                znak = -1;
            }

            log(String.Format("ChangeSummChecks: amountReceipt {0} amount {1} Delta {2}", amountReceipt, amount, Delta));

            Query = String.Format(
                "SELECT rp.cash_id, rp.id, rp.real_amount Amount, rp.name FROM receipt r \r\n" +
                "INNER JOIN receipt_item rp ON rp.receipt_header=r.id AND rp.cash_id=r.cash_id \r\n" +
                "WHERE r.cash_id={0} AND r.id={1}"
                , CashID, CheckID);

            rdMySQL = ExecuteReaderMySql(Query);
            if (rdMySQL == null)
            {
                log(String.Format("ChangeSummChecks: Ошибка при чтении содержимого Чека из MySQL {0}", ErrStr));
                return;
            }
            iRecItem = 0;
            while (rdMySQL.Read())
            {
                obj = String.Format("{0}", rdMySQL["ID"].GetType());
                vRecItem[iRecItem].cash_id = (Int32)rdMySQL["Cash_ID"];
                vRecItem[iRecItem].id = (UInt64)rdMySQL["ID"];
                vRecItem[iRecItem].Tovar = rdMySQL["name"].ToString().Trim();
                vRecItem[iRecItem].amount = (decimal)rdMySQL["Amount"];
                iRecItem++;
            }
            rdMySQL.Close();

            item = 0;
            while (Delta != (decimal)0)
            {
                for (int i = 0; i < iRecItem; i++)
                {
                    if (vRecItem[i].amount < (decimal)0.1) continue;
                    item++;
                    vRecItem[i].amount = vRecItem[i].amount - (znak * (decimal)0.01);
                    Delta = Delta - (decimal)0.01;

                    if (Delta == (decimal)0) break;
                }
            }

            if (item > iRecItem) item = iRecItem;

            for (int i = 0; i < item; i++)
            {
                log(String.Format("          amount {0,10:F2} Tovar {1}", vRecItem[i].amount, vRecItem[i].Tovar));
                Query = String.Format("UPDATE receipt_item set real_amount={0} WHERE cash_id={1} AND id={2}", vRecItem[i].amount.ToString().Replace(",","."), vRecItem[i].cash_id, vRecItem[i].id);
                if (!ExecuteNonQueryMySql(Query))
                {
                    log(String.Format("ChangeSummChecks: Ошибка при исправлении содержимого Чека в MySQL {0}", ErrStr));
                    return;
                }
            }

            Query = String.Format("UPDATE receipt set amount={0} WHERE cash_id={1} AND id={2}", amount.ToString().Replace(",", "."), CashID, CheckID);
            if (!ExecuteNonQueryMySql(Query))
            {
                log(String.Format("ChangeSummChecks: Ошибка при исправлении содержимого Чека в MySQL {0}", ErrStr));
                return;
            }

        }

        void FixErrorDiffSumm(SqlConnection cn)
        {
            SqlDataReader rd = GetDifferencesSum(con);
            if (rd == null)
            {
                log(String.Format("Ошибка при чтении итоговых сумм {0}", ErrStr));
                return;
            }

//            string Query;
            string Query, Err_Str = "";
            object obj;

            MySqlDataReader rdMySQL;
//            RecItem[] vRecItem = new RecItem[300];
//            int iRecItem=0;

            while (rd.Read())
            {
                string CashID="";
                string CheckID="";
                string Amount="";

                Decimal amount, amount_p;

                Query = String.Format(
                "SELECT * FROM receipt r  \r\n" +
                "WHERE r.cash_number={0} AND r.local_number={1}  \r\n" +
                " AND r.shift_open={2}  \r\n" +
//                " AND r.ext_processed =0  \r\n" +
                " ORDER BY r.date DESC;  \r\n" +
                "  \r\n", rd["CASHNUMBER"].ToString(), rd["CHECKNUMBER"].ToString(), rd["ZNUMBER"].ToString());

                rdMySQL = ExecuteReaderMySql(Query);
                if (rdMySQL == null)
                {
                    rd.Close();
                    log(String.Format("FixErrorDiffSumm: Ошибка при чтении Чека из MySQL {0}", ErrStr));
                    return;
                }

                while (rdMySQL.Read())
                {
                    CashID = rdMySQL["Cash_ID"].ToString();
                    CheckID = rdMySQL["ID"].ToString();
                    Amount = rdMySQL["Amount"].ToString().Replace(",",".");
                    break;
                }
                rdMySQL.Close();

                ChangeSummChecks( CashID, CheckID);
/*
                string PaymentPrioritet = Program.GetValue("PaymentPrioritet","1");

                if (PaymentPrioritet == "1")
                {
                    Query = String.Format(
                    "SELECT * FROM receipt_payment rp  \r\n" +
                    "WHERE rp.cash_id={0} AND rp.receipt_header={1} AND rp.type IN (0,2) AND rp.type IN (0,2);  \r\n" +
                    "  \r\n", CashID, CheckID);

                    rdMySQL = ExecuteReaderMySql(Query);
                    if (rdMySQL == null)
                    {
                        rd.Close();
                        log(String.Format("FixErrorDiffSumm: Ошибка при чтении оплат по Чеку из MySQL {0}", ErrStr));
                        return;
                    }

                    while (rdMySQL.Read())
                    {
                        CashID = rdMySQL["Cash_ID"].ToString();
                        CheckID = rdMySQL["ID"].ToString();
                        Amount = rdMySQL["Amount"].ToString().Replace(",", ".");
                        break;
                    }
                    rdMySQL.Close();

                    amount_p = Convert.ToDecimal(Amount);

                    Query = String.Format(
                        "SELECT SUM(rp.amount) amount FROM receipt r \r\n" +
                        "INNER JOIN receipt_payment rp ON rp.receipt_header=r.id AND rp.cash_id=r.cash_id \r\n" +
                        "WHERE r.cash_id={0} AND r.id={1} AND rp.type IN (0,1,4)"
                        , CashID, CheckID);
                    if (!ExecuteScalarMySql(Query, out obj, out ErrStr))
                    {
                        log(String.Format("ChangeSummChecks: Ошибка при чтении итоговых сумм {0}", Err_Str));
                        return;
                    }

                    amount = (Decimal)obj;

                    if (amount != amount_p)
                    {

                        Query = String.Format(
                            "SELECT rp.cash_id, rp.id, rp.real_amount Amount, rp.name FROM receipt r \r\n" +
                            "INNER JOIN receipt_item rp ON rp.receipt_header=r.id AND rp.cash_id=r.cash_id \r\n" +
                            "WHERE r.cash_id={0} AND r.id={1}"
                            , CashID, CheckID);

                        rdMySQL = ExecuteReaderMySql(Query);
                        if (rdMySQL == null)
                        {
                            log(String.Format("ChangeSummChecks: Ошибка при чтении содержимого Чека из MySQL {0}", ErrStr));
                            return;
                        }
                        iRecItem = 0;
                        vRecItem = new RecItem[300];
                        while (rdMySQL.Read())
                        {
                            obj = String.Format("{0}", rdMySQL["ID"].GetType());
                            vRecItem[iRecItem].cash_id = (Int32)rdMySQL["Cash_ID"];
                            vRecItem[iRecItem].id = (UInt64)rdMySQL["ID"];
                            vRecItem[iRecItem].Tovar = rdMySQL["name"].ToString().Trim();
                            vRecItem[iRecItem].amount = (decimal)rdMySQL["Amount"];
                            iRecItem++;
                        }
                        rdMySQL.Close();
                    }

                    int znak = 0;
                    Decimal Delta = (amount - amount_p);
                    if (Delta < (decimal)0)
                    {
                        Delta = -Delta;
                        znak = -1;
                    }
                    
                    int item = 0;
                    while (Delta != (decimal)0)
                    {
                        for (int i = 0; i < iRecItem; i++)
                        {
                            if (vRecItem[i].amount < (decimal)0.1) continue;
                            item++;
                            vRecItem[i].amount = vRecItem[i].amount - (znak * (decimal)0.01);
                            Delta = Delta - (decimal)0.01;

                            if (Delta == (decimal)0) break;
                        }
                    }


                    if (item > iRecItem) item = iRecItem;

                    for (int i = 0; i < item; i++)
                    {
                        log(String.Format("          amount {0,10:F2} Tovar {1}", vRecItem[i].amount, vRecItem[i].Tovar));
                        Query = String.Format("UPDATE receipt_item set real_amount={0} WHERE cash_id={1} AND id={2}", vRecItem[i].amount.ToString().Replace(",", "."), vRecItem[i].cash_id, vRecItem[i].id);
                        if (!ExecuteNonQueryMySql(Query))
                        {
                            log(String.Format("ChangeSummChecks: Ошибка при исправлении содержимого Чека в MySQL {0}", ErrStr));
                            return;
                        }
                    }

                    Query = String.Format("UPDATE receipt set amount={0} WHERE cash_id={1} AND id={2}", amount_p.ToString().Replace(",", "."), CashID, CheckID);
                    if (!ExecuteNonQueryMySql(Query))
                    {
                        log(String.Format("FixErrorDiffSumm: Ошибка при исправлении содержимого Чека в MySQL {0}", ErrStr));
                        return;
                    }


                    //Query = String.Format(
                    //"UPDATE  receipt_payment SET amount={0}  \r\n" +
                    //"WHERE Cash_ID={1} AND id={2} AND `type`=0;  \r\n", Amount, CashID, CheckID);
                    //if (!ExecuteNonQueryMySql(Query))
                    //{
                    //    log(String.Format("FixErrorDiffSumm: Ошибка при корректировке amount в receipt_payment {0}", ErrStr));
                    //}
                }
                else
                {
                    Query = String.Format(
                    "SELECT * FROM receipt_payment rp  \r\n" +
                    "WHERE rp.cash_id={0} AND rp.receipt_header={1};  \r\n" +
                    "  \r\n", CashID, CheckID);

                    rdMySQL = ExecuteReaderMySql(Query);
                    if (rdMySQL == null)
                    {
                        rd.Close();
                        log(String.Format("FixErrorDiffSumm: Ошибка при чтении оплат по Чеку из MySQL {0}", ErrStr));
                        return;
                    }

                    while (rdMySQL.Read())
                    {
                        CashID = rdMySQL["Cash_ID"].ToString();
                        CheckID = rdMySQL["ID"].ToString();
                        //Amount = rdMySQL["Amount"].ToString();
                        break;
                    }
                    rdMySQL.Close();

                    Query = String.Format(
                    "UPDATE  receipt_payment SET amount={0}  \r\n" +
                    "WHERE Cash_ID={1} AND id={2};  \r\n", Amount, CashID, CheckID);
                    if (!ExecuteNonQueryMySql(Query))
                    {
                        log(String.Format("FixErrorDiffSumm: Ошибка при корректировке amount в receipt_payment {0}", ErrStr));
                    }
                }
*/

                //Query = String.Format(
                //"SELECT * FROM receipt_payment rp  \r\n" +
                //"WHERE rp.cash_id={0} AND rp.receipt_header={1} AND rp.type IN (0,2);  \r\n" +
                //"  \r\n", CashID, CheckID);

                //rdMySQL = ExecuteReaderMySql(Query);
                //if (rdMySQL == null)
                //{
                //    rd.Close();
                //    log(String.Format("FixErrorDiffSumm: Ошибка при чтении оплат по Чеку из MySQL {0}", ErrStr));
                //    return;
                //}

                //while (rdMySQL.Read())
                //{
                //    CashID = rdMySQL["Cash_ID"].ToString();
                //    CheckID = rdMySQL["ID"].ToString();
                //    //Amount = rdMySQL["Amount"].ToString();
                //    break;
                //}
                //rdMySQL.Close();

                //Query = String.Format(
                //"UPDATE  receipt_payment SET amount={0}  \r\n" +
                //"WHERE Cash_ID={1} AND id={2};  \r\n", Amount, CashID, CheckID);
                //if( !ExecuteNonQueryMySql(Query))
                //{
                //    log(String.Format("FixErrorDiffSumm: Ошибка при корректировке amount в receipt_payment {0}", ErrStr));
                //}


            }
            rd.Close();

        }

        bool CheckCheks()
        {
            object obj;
            string Query;

            Program.ExecuteSqlNonQuery(con, "TRUNCATE TABLE i_UKM_CASHPAY", out obj, out ErrStr);
            Program.ExecuteSqlNonQuery(con, "TRUNCATE TABLE i_UKM_CASHSAIL", out obj, out ErrStr);
            Program.ExecuteSqlNonQuery(con, "INSERT INTO i_UKM_CASHPAY SELECT * FROM #i_CASHPAY", out obj, out ErrStr);
            Program.ExecuteSqlNonQuery(con, "INSERT INTO i_UKM_CASHSAIL SELECT * FROM #i_CASHSAIL", out obj, out ErrStr);

            Program.ExecuteSqlNonQuery(con, "TRUNCATE TABLE i_UKM_CASHPAY_DISCOUNT", out obj, out ErrStr);
            Program.ExecuteSqlNonQuery(con, "TRUNCATE TABLE i_UKM_CASHSAIL_DISCOUNT", out obj, out ErrStr);
            Program.ExecuteSqlNonQuery(con, "INSERT INTO i_UKM_CASHPAY_DISCOUNT SELECT * FROM #i_CashPayDiscount", out obj, out ErrStr);
            Program.ExecuteSqlNonQuery(con, "NSERT INTO i_UKM_CASHSAIL_DISCOUNT SELECT * FROM #i_CashSailDiscount", out obj, out ErrStr);

            SqlDataReader rd = Program.ExecuteSqlReader(con,
                "	SELECT					\r\n" +
                "	   ch.Name				\r\n" + 
                "	   , COUNT(*) as c		\r\n" +
                "	FROM #i_CashPay as t	\r\n" +
                "	INNER JOIN Cash as ch (NOLOCK) ON ch.CashID=t.CashNumber  \r\n" +
                "	GROUP BY  \r\n" +
                "	   ch.Name  \r\n" +
                "   ORDER BY ch.Name"
                , out ErrStr
                );
            if (rd == null)
            {
                log(String.Format("Ошибка при чтении количества чеков по кассам {0}",ErrStr));
                return false;
            }
            log(" ");
            while (rd.Read())
            {
                log(String.Format("Загрузка оперсводки для {0} - {1}",rd["Name"],rd["c"]));
            }
            rd.Close();

            if (!Program.ExecuteSqlScalar(con, "SELECT COUNT(*) FROM #i_CashPay", out obj, out ErrStr))
            {
                log(String.Format("Ошибка при чтении количества чеков : {0}", ErrStr));
                return false;
            }
            log(" ");
            log(String.Format("Количество   чеков : {0}", (int)obj));

            if (!Program.ExecuteSqlScalar(con, "SELECT COUNT(*) FROM #i_CashSail", out obj, out ErrStr))
            {
                log(String.Format("Ошибка при чтении количества позиций : {0}", ErrStr));
                return false;
            }
            log(String.Format("Количество позиций : {0}", (int)obj));

            Query =
        "SELECT											\r\n" +
        "	ROUND(SUM(TOTALRUB),2) as TOTALRUB,			\r\n" +
        "	ROUND(SUM(PAYEDRUB),2) as PAYEDRUB			\r\n" +
        "FROM											\r\n" +
        "(												\r\n" +
        "	SELECT										\r\n" +
        "		ROUND(SUM(TOTALRUB),2) as TOTALRUB,		\r\n" +
        "		0 as PAYEDRUB							\r\n" +
        "	FROM #i_CASHSAIL							\r\n" +
        "												\r\n" +
        "	UNION ALL									\r\n" +
        "												\r\n" +
        "	SELECT										\r\n" +
        "		0 as TOTALRUB,							\r\n" +
        "		ROUND(SUM(PAYEDRUB),2) as PAYEDRUB		\r\n" +
        "	FROM #i_CASHPAY								\r\n" +
        ") Z											";
            rd = Program.ExecuteSqlReader(con, Query, out ErrStr);
            if (rd == null)
            {
                log(String.Format("Ошибка при чтении итоговых сумм : {0}", ErrStr));
                return false;
            }
            while (rd.Read())
            {
                if ((decimal)rd["TOTALRUB"] != (decimal)rd["PAYEDRUB"])
                {
                    log(String.Format("Не совпадают итоговые суммы по чекам {0} и позициям {1}", (decimal)rd["PAYEDRUB"], (decimal)rd["TOTALRUB"]));
                    rd.Close();

                    rd = GetDifferencesSum(con);

                    if (rd != null)
                    {
                        while (rd.Read())
                        {
                            object o = rd["CASHNUMBER"];
                            log(String.Format("     Не совпадают итоговые суммы по кассе {0,2}  Чек № {1,4} сумма в чеке {2,9} по позициям {3,9}", rd["CASHNUMBER"], rd["CHECKNUMBER"], (decimal)rd["PAYEDRUB"], (decimal)rd["TOTALRUB"]));
                        }
                        rd.Close();
                    }

                    //
                    // Попытаемся исправить суммы в таблице receipt_payment конвертора
                    //
                    FixErrorDiffSumm(con);

                    return false;
                }
            }
            rd.Close();

            return true;
        }

        void ImportDataLine(CLineImport v)
        {
            string Table, Query, errStr;
            object obj, res;
            int iCnt = 0;
            int iSoftChecks = 0;

            GetUkmVersion();

            CheckIndex("receipt", "R_CASH_ID", "CREATE INDEX R_CASH_ID ON receipt (cash_id, id);");
            CheckIndex("shift", "SH_CASH_ID", "CREATE INDEX SH_CASH_ID ON shift (cash_id, id);");
            CheckIndex("shift", "SH_EXT_STATUS", "CREATE INDEX SH_EXT_STATUS ON shift (ext_status, close_date);");
            CheckIndex("shift", "SH_CASH_NUM", "CREATE INDEX SH_CASH_NUM ON shift (cash_number, id);");
            CheckIndex("receipt_payment", "_IX_RP_FULL", "CREATE INDEX _IX_RP_FULL ON receipt_payment(cash_id,receipt_header);");
            CheckIndex("receipt_item_tax", "_IX_RIT_FULL", "CREATE INDEX _IX_RIT_FULL ON receipt_item_tax(cash_id,receipt_item);");

            //if (TableExistMySql("Checks"))
            //{
            if (!ExecuteNonQueryMySql("DROP TABLE IF EXISTS Checks;"))
                {
                    log(String.Format("Ошибка при удалении Checks error: {0}",ErrStr));
                    return;
                }
           // }
            if (!ExecuteNonQueryMySql(Properties.Resources.CREATE_Checks))
            {
                    log(String.Format("Ошибка при создании Checks error: {0}",ErrStr));
                    return;
            }
            if (!ExecuteNonQueryMySql("CREATE INDEX IX_CHECK ON checks(cash_id,id);"))
            {
                log(String.Format("Ошибка при создании index Checks error: {0}", ErrStr));
                return;
            }

            //if (TableExistMySql("shift_t"))
            //{
            if (!ExecuteNonQueryMySql("DROP TABLE IF EXISTS shift_t;"))
                {
                    log(String.Format("Ошибка при удалении shift_t error: {0}", ErrStr));
                    return;
                }
            //}
            if (!ExecuteNonQueryMySql(Properties.Resources.CREATE_shift_t))
            {
                log(String.Format("Ошибка при создании shift_t error: {0}", ErrStr));
                return;
            }

            //
            // Чтение чеков и смен
            if (!ExecuteNonQueryMySql("call GetChecks;"))
            {
                log(String.Format("Нет доступных чеков error: {0}", ErrStr));
                return;
            }
            nChecks = nRows;

            if (!ExecuteNonQueryMySql("call GetShift;"))
            {
                log(String.Format("Нет доступных смен error: {0}", ErrStr));
                return;
            }
            nShift = nRows;

            if ((nChecks == 0) && (nShift == 0)) return;

//            log(" ");
//            log(String.Format("     nChecks={0}", nChecks));
            log(" ");
            log(String.Format("Версия УКМ-Сервера {0} ({1}) обновлен {2}", ukm_Verion, fullUkmVersion, ukm_dateUpdate));
            log(String.Format("====== ИМПОРТ ЧЕКОВ И СМЕН ИЗ КАССОВОЙ ЛИНЕЙКИ '{0}' ============",v.Line));
            log(" ");
            log(String.Format("Command Timeout Для MySQL: {0}", timeOut.ToString()));
            log(" ");

            if (!DeleteTemporaryTable())
            {
                return;
            }

            if (!CreateTemporaryTable())
            {
                return;
            }

            log("Создали временные таблицы");

            CheckIndexSMS("Checks", "_IX_Checks_Dat_No", "if object_id('_IX_Checks_Dat_No') is null    \r\n  CREATE INDEX _IX_Checks_Dat_No ON Checks(CashID,CashCheckNo,CloseDate)");
            CheckIndexSMS("Check_Order_Order", "_IX_Check_Order_Order_ID", "if object_id('_IX_Check_Order_Order_ID') is null    \r\n  CREATE INDEX _IX_Check_Order_Order_ID ON Check_Order_Order(CheckID)");

            //=====================================================================================================
            //  Получение идентификатора магазина для выборки из ukmserver.trm_in_order_order
            //=====================================================================================================
            string Store_Id = "15001";
            if (ExecuteScalarMySql("SELECT COUNT(*) c FROM ukmserver.trm_in_order_order", out res, out errStr))
            {
                if (Convert.ToInt32(res.ToString()) != 0)
                {
                    if (ExecuteScalarMySql("SELECT store_id FROM ukmserver.trm_in_order_order LIMIT 1", out res, out errStr))
                    {
                        Store_Id = res.ToString().Trim();
                    }
                }
            }
            //=====================================================================================================

            if (DebugLevel != 0)
                log("Начало копирования таблиц из MYSQL");

            if (!CopyMySqlToSms("#u_receipt", Properties.Resources.Create_u_receipt)) return;
            if (DebugLevel != 0)
                log(String.Format("receipt OK ({0} записей)", nRows));
            //
            if (!CopyMySqlToSms("#u_shift", Properties.Resources.Create_u_shift)) return;
            if (DebugLevel != 0)
                log(String.Format("shift OK ({0} записей)", nRows));
            //
            if (!CopyMySqlToSms("#u_login", Properties.Resources.Create_u_login)) return;
            if (DebugLevel != 0)
                log(String.Format("login OK ({0} записей)", nRows));
            //
            if (!CopyMySqlToSms("#u_receipt_item", Properties.Resources.Create_u_receipt_item)) return;
            if (DebugLevel != 0)
                log(String.Format("receipt_item OK ({0} записей)", nRows));
            //
            if (!CopyMySqlToSms("#u_receipt_item_tax", Properties.Resources.Create_u_receipt_item_tax)) return;
            if (DebugLevel != 0)
                log(String.Format("u_receipt_item_tax OK ({0} записей)", nRows));
            //
            if (!CopyMySqlToSms("#trm_out_receipt_egais", Properties.Resources.Create_trm_out_receipt_egais)) return;
            if (DebugLevel != 0)
                log(String.Format("ukmserver.trm_out_receipt_egais OK ({0} записей)", nRows));
            //
            if (!CopyMySqlToSms("#u_receipt_payment", Properties.Resources.Create_u_receipt_payment)) return;
            if (DebugLevel != 0)
                log(String.Format("receipt_payment OK ({0} записей)", nRows));
            //
            if (!CopyMySqlToSms("#u_receipt_item_discount", Properties.Resources.Create_u_receipt_item_discount)) return;
            if (DebugLevel != 0)
                log(String.Format("receipt_item_discount OK ({0} записей)", nRows));
            //
            if (!CopyMySqlToSms("#u_receipt_discounts", Properties.Resources.Create_u_receipt_discount)) return;
            if (DebugLevel != 0)
                log(String.Format("receipt_discounts OK ({0} записей)", nRows));
            //
            Query = Properties.Resources.Create_u_receipt_order_order;
            Query = Query.Replace("%store_id%", Store_Id);
            if (!CopyMySqlToSms("#u_receipt_order_order", Query)) return;
            iSoftChecks = nRows;
            if (DebugLevel != 0)
                log(String.Format("u_receipt_order_order OK ({0} записей)", nRows));

            if (Int32.Parse(ukm_Verion) < 74)
            {
                //==============================================================================
                //  Формируем временную таблицу #receipt_item_addition
                //    в которой содержатся отсканированные акцизные марки
                //    по алкоголю
                Table = "#receipt_item_addition";
                Query = Properties.Resources.s9_i_item_addition;
            }
            else
            {
                //==============================================================================
                //  Формируем временную таблицу #receipt_item_addition
                //    в которой содержатся отсканированные акцизные марки
                //    по алкоголю
                Table = "#receipt_item_addition";
                Query = Properties.Resources.s9_i_item_egais;
            }
            if (!CopyMySqlToSms(Table, Query)) return;
            if (DebugLevel != 0)
                log(String.Format("receipt_item_addition OK ({0} записей)", nRows));

            log("Скопировали данные из MySQL");

            //-----------------------------------------------------------
            //-- Заполнение временных таблиц
            //--	(#i_Shift, #Shift, #i_CashPay, #i_CashSail, 
            //--	 #i_CashDisc, #i_CashDcrd, #i_CashAuth
            //--	) 
            //-----------------------------------------------------------
            if( !Program.ExecuteSqlScalar(con,"exec i_UKM_Imort_Prepare", out obj, out ErrStr) )
            {
                log(String.Format("Ошибка при выполнении i_UKM_Imort_Prepare : {0}", ErrStr));
                return;
            }
            log("i_UKM_Imort_Prepare = OK");
            //
            if (!Program.ExecuteSqlScalar(con, "SELECT COUNT(*) FROM #i_Shift", out obj, out ErrStr))
            {
                log(String.Format("Ошибка при чтении количества смен : {0}", ErrStr));
                return;
            }
            log(String.Format("Количество смен : {0}", (int)obj));

            if (nChecks > 0)
                if (!CheckCheks()) return;

            if (iSoftChecks != 0)
            {
                log(" ");
                log(String.Format("Количество мягких чеков : {0}", iSoftChecks));
            }

            log(" ");

            SqlDataReader rd = Program.ExecuteSqlReader(con,
                "	SELECT  \r\n" +
                "	   ch.Name  \r\n" +
                "	FROM #i_Shift as t  \r\n" +
                "	INNER JOIN Cash as ch (NOLOCK) ON ch.CashID=t.CashNumber  \r\n" +
                "   WHERE t.CloseDate is NOT NULL  \r\n" +
                "	ORDER BY  \r\n" +
                "	   ch.Name"
                , out ErrStr);
            if (rd == null)
            {
                log(String.Format("Ошибка при чтении закрытых смен : {0}", ErrStr));
                return;
            }
            iCnt = 0;
            while (rd.Read())
            {
                log(String.Format("Закрытие смены для {0}", rd["Name"]));
                iCnt++;
            }
            rd.Close();

            if (iCnt > 0) log(" ");
           
            if (!Program.ExecuteSqlScalar(con, "exec [dbo].[i_UKM_Imort]", out obj, out ErrStr))
            {
                log(String.Format("Ошибка при выполнении i_UKM_Imort : {0}", ErrStr));
                return;
            }
            log("i_UKM_Imort выполнена успешно");

            if (!ExecuteNonQueryMySql(
                    "UPDATE receipt,CHECKS									   \r\n" +
            		"	SET ext_processed=1									   \r\n"+
		            "WHERE receipt.cash_id=CHECKS.cash_id AND receipt.id=CHECKS.id;"
                        )
                )
            {
                log(String.Format("Ошибка UpdateChecks: {0}", ErrStr));
                return;
            }

            if (!ExecuteNonQueryMySql(
            		"UPDATE shift,CHECKS		 \r\n" +
		            "	SET ext_status=3		 \r\n" +
		            "WHERE shift.id=CHECKS.shift_open AND shift.close_date is NULL;"
                        )
                )
            {
                log(String.Format("Ошибка UpdateOper: {0}", ErrStr));
                return;
            }

            if (!ExecuteNonQueryMySql(
            		"UPDATE shift,shift_t			\r\n" +
                    "	SET ext_status=1			\r\n" +
		            "WHERE shift.id=shift_t.id AND shift.close_date is NOT NULL"
                        )
                )
            {
                log(String.Format("Ошибка UpdateShift: {0}", ErrStr));
                return;
            }


            if (Program.ExecuteSqlNonQuery(con, "if object_id('tempdb..#i_order_order') is not null     DROP TABLE #i_order_order", out obj, out ErrStr))
            {
                if (Program.ExecuteSqlNonQuery(con, Properties.Resources.Create_i_order_order, out obj, out ErrStr))
                {
                    if (Program.ExecuteSqlNonQuery(con, "DELETE FROM Check_Order_Order WHERE CheckID IN (SELECT CheckID FROM #i_order_order)", out obj, out ErrStr))
                    {
                        if (Program.ExecuteSqlNonQuery(con, Properties.Resources.Insert_Check_Order_Order, out obj, out ErrStr))
                        {
                            //log(String.Format("Ошибка при создании {0} = {1}", TableName, ErrStr));
                            //return false;
                        }
                        else
                        {
                            log(String.Format("Ошибка при добавлении в Check_Order_Order = {0}", ErrStr));
                        }
                    }
                    else
                    {
                        log(String.Format("Ошибка при удалении из Check_Order_Order = {0}", ErrStr));
                    }
                }
                else
                {//#i_order_order
                    log(String.Format("Ошибка при создании #i_order_order = {0}", ErrStr));
                }
            }

            log(" ");
            log("Данные импортированы");
            log(" ");
        }

        public void Import()
        {
            DebugLevel = Int32.Parse(Program.GetValue("DebugLevel", "0"));

            try
            {
                if (!Open()) return;

                foreach (CLineImport v in vLine)
                {
                    workDB = v.ImportDb;
                    mDB.ChangeDatabase(workDB.Trim());
                    ImportDataLine(v);
                }

                Close();
            }
            catch (Exception e)
            {
                log(String.Format("Import *ERROR*: {0}", e.ToString()));
                //Environment.Exit(-1);
                return;
            }

        }
    }
}
