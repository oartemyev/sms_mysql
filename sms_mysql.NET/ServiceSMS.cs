using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Data;
using System.Data.SqlClient;

using System.Diagnostics;
using System.ServiceProcess;
using System.Threading;

namespace sms_mysql.NET
{
    class ServiceSMS : ServiceBase
    {
        private string svcName;
        private ServiceBody srv;

        public ServiceSMS(string srvName = "sms_mysql.NET")
            : base()
        {
            svcName = srvName;
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            // TODO: Add code here to start your service.
            srv = new ServiceBody();
            srv.ServiceStart(args);
        }

        protected override void OnStop()
        {
            // TODO: Add code here to perform any tear-down necessary to stop your service.
            Program.killAll = true;
            srv.ServiceStop();
        }
        /// <summary> 
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            components = new System.ComponentModel.Container();
            this.ServiceName = "ServiceSMS";
        }

        #endregion
    }

    public class ServiceBody
    {
        public Thread mainThread;

        public int iSmukmVer, MainID;
        public string ImportData, ExportData;

        public void ServiceStart(string[] args)
        {
            Program.WriteLog("");
            Program.WriteLog("==============================================================================");
            Program.WriteLog("Сервис SMS запущен");

            mainThread = new Thread(DoWork);
            mainThread.Start(this);
        }

        public void ServiceStop()
        {
            mainThread.Join();

            Program.WriteLogName(Program.logNameExport, "Kill (Экспорт данных из УКМ используя MySQL) detected");
            Program.WriteLogName(Program.logNameExport, "Thread(Экспорт данных из УКМ используя MySQL) end");

            Program.WriteLogName(Program.logNameImport, "Kill (Импорт данных из УКМ используя MySQL) detected");
            Program.WriteLogName(Program.logNameImport, "Thread(Импорт данных из УКМ используя MySQL) end");

            Program.WriteLog("Cервис остановлен");
        }

        bool GetInfo()
        {
            string errString;
            SqlConnection con;
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
                Program.WriteLog(String.Format("Ошибка при открытии SMS: {0}", e.ToString()));
                if(Program.bDebugMode)
                    Program.killAll = true;
                //throw;
                return false;
            }

            SqlDataReader reader = Program.ExecuteSqlReader(con, "exec i_getSMSDBInfo", out errString);
            if (reader == null)
            {
                Program.WriteLog(String.Format("Ошибка при чтении версии SMS: {0}", errString));
                if (Program.bDebugMode)
                    Program.killAll = true;
                return false;
            }
            while (reader.Read())
            {
                if (reader["Name"].ToString().ToUpper() == "SMS")
                {
                    Program.WriteLog(String.Format("Версия SMS ={0}.{1}", reader["Rev"], reader["Major"]));
                }
            }
            reader.Close();

            reader = Program.ExecuteSqlReader(con, "select * from i_Managers", out errString);
            if (reader == null)
            {
                Program.WriteLog(String.Format("Ошибка при чтении списка менеджеров: {0}", errString));
                if (Program.bDebugMode)
                    Program.killAll = true;
                return false;
            }
            Program.WriteLog("Список зарегистированных менеджеров:");
            while (reader.Read())
            {
                Program.WriteLog(String.Format("\t{0}", reader[1]));
            }
            reader.Close();


            reader = Program.ExecuteSqlReader(con, "select * from Constants WHERE Code = \'SMUKM_Ver\'", out errString);
            if (reader == null)
            {
                Program.WriteLog(String.Format("Ошибка при чтении версии SMUKM: {0}", errString));
                if (Program.bDebugMode)
                    Program.killAll = true;
                return false;
            }
            while (reader.Read())
            {
                Program.WriteLog(String.Format("Версия SMUKM-драйвера   {0}", reader[2]));
                iSmukmVer = Int32.Parse(reader[2].ToString());
            }
            reader.Close();
            if (iSmukmVer == 0)
            {
                Program.WriteLog("Ошибка при чтении версии SMUKM");
                con.Close();
                if (Program.bDebugMode)
                    Program.killAll = true;
                return false;
            }

            if (iSmukmVer != 4)
            {
                Program.WriteLog("Сервис работате только с версией SMUKM 4");
                con.Close();
                Program.killAll = true;
                return true;
            }

            object id;
            if (!Program.ExecuteSqlScalar(con, "SELECT ID FROM i_Managers where Name=\'Супермаг УКМ4\'", out id, out errString))
            {
                Program.WriteLog("Ошибка при чтении ID \'Супермаг УКМ4\'");
                con.Close();
                if (Program.bDebugMode)
                    Program.killAll = true;
                return false;
            }
            MainID = (int)id;
            //Program.WriteLog(String.Format("ID \'Супермаг УКМ4\' {0} {1}",srv.MainID, id));

            string Query = String.Format(
                "select distinct l.Name from Line l " +
                "Inner Join CashLines cl ON l.LineID=cl.LineID  " +
                "Inner Join Cash c ON cl.CashID=c.CashID " +
                "Inner Join i_ManagerCashes mc ON cl.CashID = mc.CashID " +
                "WHERE mc.ManagerID={0}", MainID);
            reader = Program.ExecuteSqlReader(con, Query, out errString);
            if (reader == null)
            {
                Program.WriteLog(String.Format("Ошибка при чтении версии SMUKM: {0}", errString));
                if (Program.bDebugMode)
                    Program.killAll = true;
                return false;
            }
            Program.WriteLog("Список зарегистированных кассовых линеек:");
            while (reader.Read())
            {
                Program.WriteLog(String.Format("\t{0}", reader["Name"]));
            }
            reader.Close();

            Query = String.Format(
                "select distinct c.Name from Line l " +
                "Inner Join CashLines cl ON l.LineID=cl.LineID  " +
                "Inner Join Cash c ON cl.CashID=c.CashID " +
                "Inner Join i_ManagerCashes mc ON cl.CashID = mc.CashID " +
                "WHERE mc.ManagerID={0}", MainID);
            reader = Program.ExecuteSqlReader(con, Query, out errString);
            if (reader == null)
            {
                Program.WriteLog(String.Format("Ошибка при чтении версии SMUKM: {0}", errString));
                if (Program.bDebugMode)
                    Program.killAll = true;
                return false;
            }
            Program.WriteLog("Список зарегистированных касс:");
            while (reader.Read())
            {
                Program.WriteLog(String.Format("\t{0}", reader["Name"]));
            }
            reader.Close();


            ImportData = Program.cfg["ImportData"];
            if (ImportData.ToUpper() == "DBF")
            {
                Program.WriteLog("Импорт из УКМ через DBF-файлы");
                //importThread = new Thread(DbfConverter.ImportDbf);
                //importThread.Start();
            }
            else if (ImportData.ToUpper() == "MYSQL")
            {
                Program.WriteLog("Импорт из УКМ через MySQL");
                //importThread = new Thread(MySqlConverter.ImportMySql);
                //importThread.Start();
            }
            else
            {
                Program.WriteLog(String.Format("Не верный конвертер импорта из УКМ {0}", ImportData));
                Program.killAll = true;
                return true;
            }

            ExportData = Program.cfg["ExportData"];
            if (ExportData.ToUpper() == "DBF")
            {
                Program.WriteLog("Экспорт в УКМ через DBF-файлы");
                //exportThread = new Thread(DbfConverter.ExportDbf);
                //exportThread.Start();
            }
            else if (ExportData.ToUpper() == "MYSQL")
            {
                Program.WriteLog("Экспорт в УКМ через MySQL");
                //exportThread = new Thread(MySqlConverter.ExportMySql);
                //exportThread.Start();
            }
            else
            {
                Program.WriteLog(String.Format("Не верный конвертер экспорта в УКМ {0}", ImportData));
                Program.killAll = true;
                return true;
            }


            con.Close();

            return true;
        }

        void RunExportData()
        {
            ExportMySQL s = new ExportMySQL(Program.logNameExport);

            s.Export();
        }

        void RunImportData()
        {
            ImportMySQL s = new ImportMySQL(Program.logNameImport);

            s.Import();
        }

        public static void DoWork(object body)
        {
            ServiceBody srv = (ServiceBody)body;
            Thread importThread = null, exportThread = null;

            //srv.GetInfo();
            while (!Program.killAll)
            {
                if (srv.GetInfo()) break;
                Thread.Sleep(10000); // Ждем 10 секунд, похоже нет подключения
            }

            exportThread = new Thread(DoExportData);
            exportThread.Start(srv);

            importThread = new Thread(DoImportData);
            importThread.Start(srv);

            while(!Program.killAll)
            {
                Thread.Sleep(500);
            }
        }

        public static void DoExportData(object body)
        {
            int nDelay = Convert.ToInt32(Program.cfg["TimeoutWriteKKM"]);
            ServiceBody srv = (ServiceBody)body;
            int nSec = 0;

            Program.WriteLogName(Program.logNameExport, " ");
            Program.WriteLogName(Program.logNameExport, "Thread( Экспорт данных из УКМ используя MySQL) beg");

            while (!Program.killAll)
            {
                if(nSec >= (nDelay * 2))
                {
                    srv.RunExportData();
                    nSec = 0;
                }
                Thread.Sleep(500);
                nSec++;
            }
            Program.WriteLogName(Program.logNameExport,"Kill (Экспорт данных из УКМ используя MySQL) detected");
            Program.WriteLogName(Program.logNameExport, "Thread(Экспорт данных из УКМ используя MySQL) end");
        }

        public static void DoImportData(object body)
        {
            int nDelay = Convert.ToInt32(Program.cfg["TimeoutReadKKM"]);
            ServiceBody srv = (ServiceBody)body;
            int nSec = 0;

            Program.WriteLogName(Program.logNameImport, " ");
            Program.WriteLogName(Program.logNameImport, "Thread( Импорт данных из УКМ используя MySQL) beg");

            while (!Program.killAll)
            {
                if(nSec >= (nDelay * 2))
                {
                    srv.RunImportData();
                    nSec = 0;
                }
                Thread.Sleep(500);
                nSec++;
            }
            Program.WriteLogName(Program.logNameImport, "Kill (Импорт данных из УКМ используя MySQL) detected");
            Program.WriteLogName(Program.logNameImport, "Thread(Импорт данных из УКМ используя MySQL) end");
        }
    }

}
