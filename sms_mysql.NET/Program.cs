using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using System.ServiceProcess;
using System.IO;

using System.Data;
using System.Data.SqlClient;

using System.Runtime.InteropServices;
using System.Runtime.InteropServices.ComTypes;

namespace sms_mysql.NET
{
    internal delegate void SignalHandler(ConsoleSignal consoleSignal);

    internal enum ConsoleSignal
    {
        CtrlC = 0,
        CtrlBreak = 1,
        Close = 2,
        LogOff = 5,
        Shutdown = 6
    }

    internal static class ConsoleHelper
    {
        [DllImport("Kernel32", EntryPoint = "SetConsoleCtrlHandler")]
        public static extern bool SetSignalHandler(SignalHandler handler, bool add);
    }

    class Program
    {
        public static Dictionary<string, string> cfg = new Dictionary<string, string>();
        public static string logNameExport, logNameImport, logMain, cfgFile;

        public static bool killAll = false;
        public static bool bDebugMode = false;

        private static SignalHandler signalHandler;
        private static int commandTimeout = 1000;
        public static int commandMySQLTimeout = 1000;

        public static int lSizeLogFile = 104857600; // 100 MB


        public static bool ExecuteSqlNonQuery(SqlConnection connection, string Query, out object nRows, out string ErrStr)
        {
            SqlCommand cmd = connection.CreateCommand();
            try
            {
                cmd.CommandTimeout = Program.commandTimeout;
                cmd.CommandText = Query;
                nRows = cmd.ExecuteNonQuery();
                ErrStr = "";
            }
            catch (Exception e)
            {
                nRows = 0;
                ErrStr = e.Message.Trim();
                //Console.WriteLine(ErrStr);
                return false;
            }

            return true;
        }

        public static bool ExecuteSqlScalar(SqlConnection connection, string Query, out object res, out string ErrStr)
        {
            SqlCommand cmd = connection.CreateCommand();
            try
            {
                cmd.CommandTimeout = Program.commandTimeout;
                cmd.CommandText = Query;
                res = cmd.ExecuteScalar();
                if (res == null) res = (int)0;
                ErrStr = "";
            }
            catch (Exception e)
            {
                res = null;
                ErrStr = e.Message.Trim();

                return false;
            }

            return true;
        }

        public static SqlDataReader ExecuteSqlReader(SqlConnection connection, string Query, out string ErrStr)
        {
            SqlCommand command = connection.CreateCommand();
            command.CommandTimeout = Program.commandTimeout; // Тайм-аут
            SqlDataReader reader;

            try
            {
                command.CommandText = Query;
                reader = command.ExecuteReader();
                ErrStr = "";
            }
            catch (Exception e)
            {
                ErrStr = e.Message.Trim();
                //Console.WriteLine(ErrStr);
                return null;
            }

            return reader;
        }

        static void Main(string[] args)
        {
            ServiceBase[] ServicesToRun;

            String path = System.Reflection.Assembly.GetExecutingAssembly().Location;
            cfgFile = Path.ChangeExtension(path, ".cfg");
            logMain = Path.ChangeExtension(path, ".log");
            logNameExport = Path.Combine(Path.GetDirectoryName(path), "Sms.WriteKKM.NET.log");
            logNameImport = Path.Combine(Path.GetDirectoryName(path), "Sms.ReadKKM.NET.log");

            Load(cfgFile);

            commandTimeout = Convert.ToInt32(Program.GetValue("CommandTimeOut", "1000"));
            lSizeLogFile = Convert.ToInt32(Program.GetValue("SizeLogFile", "104857600"));

            if (args.Length >= 2)
            {
                if ((args[0].ToLower() == "-install") || (args[0].ToLower() == "-i"))
                {
                    sms_new.SCM.CreateService(args[1]);
                    return;
                }
                else
                    if ((args[0].ToLower() == "-remove") || (args[0].ToLower() == "-r"))
                    {
                        sms_new.SCM.RemoveService(args[1]);
                        return;
                    }
                    else
                        if (args.Length >= 1)
                        {
                            if ((args[0].ToLower() == "-debug") || (args[0].ToLower() == "-d"))
                            {
                                string fileStop = Path.Combine(Path.GetDirectoryName(path), "Sms_stop.t-m");

                                bDebugMode = true;

                                WriteLog("");
                                WriteLog("==============================================================================");
                                WriteLog("Режим отладки сервиса...");
                                //Console.WriteLine("Режим отладки сервиса...");

                                signalHandler += HandleConsoleSignal;
                                ConsoleHelper.SetSignalHandler(signalHandler, true);

                                ServiceBody srv = new ServiceBody();
                                srv.ServiceStart(args);

                                while (!File.Exists(fileStop))
                                {
                                    Thread.Sleep(200);
                                }
                                File.Delete(fileStop);
                                killAll = true;

                                srv.ServiceStop();

                                WriteLog("Режим отладки сервиса остановлен");

                                return;
                            }
                        }
            }

            // More than one user Service may run within the same process. To add
            // another service to this process, change the following line to
            // create a second service object. For example,
            //
            //   ServicesToRun = new ServiceBase[] {new Service1(), new MySecondUserService()};
            //
            ServicesToRun = new ServiceBase[] { new ServiceSMS("Service1") };

            ServiceBase.Run(ServicesToRun);
        }

        private static void HandleConsoleSignal(ConsoleSignal consoleSignal)
        {
            // TO DO
            WriteLog(String.Format("ConsoleSignal = {0}", consoleSignal));
            Console.WriteLine("ConsoleSignal = {0}", consoleSignal);
            Environment.Exit(0);
        }

        public static void Load(string FileName)
        {
            string NextLine;

            cfg.Clear();

            try
            {

                if (File.Exists(FileName))
                {
                    StreamReader sr = new StreamReader(FileName, System.Text.Encoding.GetEncoding(1251));

                    while ((NextLine = sr.ReadLine()) != null)
                    {
                        if (NextLine.Trim() == "") continue;
                        if (NextLine.Trim().Substring(0, 1) == "#") continue;

                        int pos = NextLine.IndexOf(' ');
                        if (pos != -1) //NextLine[pos] = '\t';
                            NextLine = NextLine.Remove(pos, 1).Insert(pos, "\t");
                        //NextLine = NextLine.Replace("  ", " ");
                        //NextLine = NextLine.Replace("  ", " ");
                        string[] a = NextLine.Trim().Split('\t');
                        if (a.Length > 1)
                            cfg.Add(a[0].Trim(), a[1].Trim());
                    }
                    sr.Close();
                    commandMySQLTimeout = Convert.ToInt32(GetValue("MySQL_Timeout", "1000"));
                }
                else
                {
                    WriteLog("Нет файла конфигурации");
                    Environment.Exit(-1);
                }
            }
            catch (Exception e)
            {
                WriteLog(String.Format("The process Parse CFG failed: {0}", e.ToString()));
                Environment.Exit(-1);
            }

            if (!cfg.ContainsKey("TimeoutWriteKKM")) cfg.Add("TimeoutWriteKKM", "30");
            if (!cfg.ContainsKey("TimeoutReadKKM"))  cfg.Add("TimeoutReadKKM", "30");

            if (!cfg.ContainsKey("SMS_Server")) cfg.Add("SMS_Server", "");
            if (!cfg.ContainsKey("SMS_User")) cfg.Add("SMS_User", "");
            if (!cfg.ContainsKey("SMS_Passwd")) cfg.Add("SMS_Passwd", "");
            if (!cfg.ContainsKey("SMS_Baza")) cfg.Add("SMS_Baza", "");

            if (!cfg.ContainsKey("MySQL_Server")) cfg.Add("MySQL_Server", "");
            if (!cfg.ContainsKey("MySQL_User")) cfg.Add("MySQL_User", "");
            if (!cfg.ContainsKey("MySQL_Passwd")) cfg.Add("MySQL_Passwd", "");
            if (!cfg.ContainsKey("Shop_Export")) cfg.Add("Shop_Export", "20");
        }

        public static string GetValue(string key, string def="")
        {
            string res;

            if (!cfg.ContainsKey(key)) res = def;
            else res = cfg[key];

            return res;
        }

        static long GetFileSize(String fileName)
        {
            if (!File.Exists(fileName)) return -1;

            FileInfo file = new System.IO.FileInfo(fileName);
            return file.Length;
        }

        static void RotateBakFile(string FName)
        {
            int i;
            string File1 = "";
            string File2 = "";

            if (!File.Exists(FName)) return;

            for (i = 8; i > 0; i--)
            {
                File1 = FName.Trim() + "." + i.ToString("D03");
                if (File.Exists(File1)) break;
            }
            if (i == 0)
            {
                if (File.Exists(File1)) File.Delete(File1); Thread.Sleep(10);
                File.Move(FName, File1);
                // File.Delete(FName); Thread.Sleep(10);
            }
            else
            {
                if (File.Exists(File1)) File.Delete(File1); Thread.Sleep(10);
                while (i > 1)
                {
                    File1 = FName.Trim() + "." + (i - 1).ToString("D03");
                    File2 = FName.Trim() + "." + i.ToString("D03");
                    File.Move(File1, File2); Thread.Sleep(10);
                    if (File.Exists(File1)) File.Delete(File1); Thread.Sleep(10);
                }

                File.Move(FName, File1);
                if (File.Exists(File1)) File.Delete(FName); Thread.Sleep(10);
            }
        }

        public static void CheckRotate(string FName, long lSize)
        {
            if (GetFileSize(FName) > lSize)
            {
                String fn = FName.Trim() + ".BAK";
                RotateBakFile(fn);
                if (File.Exists(fn))
                {
                    File.Delete(fn); Thread.Sleep(10);
                }
                File.Move(FName, fn); Thread.Sleep(10);
            }

        }

        public static void WriteLog(string str)
        {
            StreamWriter writer;
            try
            {
                CheckRotate(logMain,lSizeLogFile);
                writer = new StreamWriter(logMain, true, System.Text.Encoding.GetEncoding(1251));
            }
            catch
            {
                return;
            }
            writer.WriteLine(String.Format("{0} {1}: {2}", DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString(), str));
            writer.Close();
        }

        public static void WriteLogName(string logName, string str)
        {
            StreamWriter writer;
            try
            {
                CheckRotate(logName, lSizeLogFile);
                writer = new StreamWriter(logName, true, System.Text.Encoding.GetEncoding(1251));
            }
            catch
            {
                return;
            }
            writer.WriteLine(String.Format("{0} {1}: {2}", DateTime.Now.ToShortDateString(), DateTime.Now.ToLongTimeString(), str));
            writer.Close();
        }
    }
}
