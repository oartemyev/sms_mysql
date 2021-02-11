using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.ComTypes;

namespace sms_new
{
    #region SERVICE_ACCESS
    [Flags]
    public enum SERVICE_ACCESS : uint
    {
        STANDARD_RIGHTS_REQUIRED = 0xF0000,
        SERVICE_QUERY_CONFIG = 0x00001,
        SERVICE_CHANGE_CONFIG = 0x00002,
        SERVICE_QUERY_STATUS = 0x00004,
        SERVICE_ENUMERATE_DEPENDENTS = 0x00008,
        SERVICE_START = 0x00010,
        SERVICE_STOP = 0x00020,
        SERVICE_PAUSE_CONTINUE = 0x00040,
        SERVICE_INTERROGATE = 0x00080,
        SERVICE_USER_DEFINED_CONTROL = 0x00100,
        SERVICE_ALL_ACCESS = (STANDARD_RIGHTS_REQUIRED |
            SERVICE_QUERY_CONFIG |
            SERVICE_CHANGE_CONFIG |
            SERVICE_QUERY_STATUS |
            SERVICE_ENUMERATE_DEPENDENTS |
            SERVICE_START |
            SERVICE_STOP |
            SERVICE_PAUSE_CONTINUE |
            SERVICE_INTERROGATE |
            SERVICE_USER_DEFINED_CONTROL)
    }
    #endregion
    #region SCM_ACCESS
    [Flags]
    public enum SCM_ACCESS : uint
    {
        STANDARD_RIGHTS_REQUIRED = 0xF0000,
        SC_MANAGER_CONNECT = 0x00001,
        SC_MANAGER_CREATE_SERVICE = 0x00002,
        SC_MANAGER_ENUMERATE_SERVICE = 0x00004,
        SC_MANAGER_LOCK = 0x00008,
        SC_MANAGER_QUERY_LOCK_STATUS = 0x00010,
        SC_MANAGER_MODIFY_BOOT_CONFIG = 0x00020,
        SC_MANAGER_ALL_ACCESS = STANDARD_RIGHTS_REQUIRED |
            SC_MANAGER_CONNECT |
            SC_MANAGER_CREATE_SERVICE |
            SC_MANAGER_ENUMERATE_SERVICE |
            SC_MANAGER_LOCK |
            SC_MANAGER_QUERY_LOCK_STATUS |
            SC_MANAGER_MODIFY_BOOT_CONFIG
    }

    public enum SERVICE_TYPE : uint
    {
        /// <summary>
        /// Driver service.
        /// </summary>
        SERVICE_KERNEL_DRIVER = 0x00000001,

        /// <summary>
        /// File system driver service.
        /// </summary>
        SERVICE_FILE_SYSTEM_DRIVER = 0x00000002,

        /// <summary>
        /// Service that runs in its own process.
        /// </summary>
        SERVICE_WIN32_OWN_PROCESS = 0x00000010,

        /// <summary>
        /// Service that shares a process with one or more other services.
        /// </summary>
        SERVICE_WIN32_SHARE_PROCESS = 0x00000020,

        /// <summary>
        /// The service can interact with the desktop.
        /// </summary>
        SERVICE_INTERACTIVE_PROCESS = 0x00000100
    }

    public enum SERVICE_START : uint
    {
        /// <summary>
        /// A device driver started by the system loader. This value is valid
        /// only for driver services.
        /// </summary>
        SERVICE_BOOT_START = 0x00000000,

        /// <summary>
        /// A device driver started by the IoInitSystem function. This value 
        /// is valid only for driver services.
        /// </summary>
        SERVICE_SYSTEM_START = 0x00000001,

        /// <summary>
        /// A service started automatically by the service control manager 
        /// during system startup. For more information, see Automatically 
        /// Starting Services.
        /// </summary>         
        SERVICE_AUTO_START = 0x00000002,

        /// <summary>
        /// A service started by the service control manager when a process 
        /// calls the StartService function. For more information, see 
        /// Starting Services on Demand.
        /// </summary>
        SERVICE_DEMAND_START = 0x00000003,

        /// <summary>
        /// A service that cannot be started. Attempts to start the service
        /// result in the error code ERROR_SERVICE_DISABLED.
        /// </summary>
        SERVICE_DISABLED = 0x00000004
    }

    public enum SERVICE_ERROR : uint
    {
        /// <summary>
        /// The startup program ignores the error and continues the startup
        /// operation.
        /// </summary>
        SERVICE_ERROR_IGNORE = 0x00000000,

        /// <summary>
        /// The startup program logs the error in the event log but continues
        /// the startup operation.
        /// </summary>
        SERVICE_ERROR_NORMAL = 0x00000001,

        /// <summary>
        /// The startup program logs the error in the event log. If the 
        /// last-known-good configuration is being started, the startup 
        /// operation continues. Otherwise, the system is restarted with 
        /// the last-known-good configuration.
        /// </summary>
        SERVICE_ERROR_SEVERE = 0x00000002,

        /// <summary>
        /// The startup program logs the error in the event log, if possible.
        /// If the last-known-good configuration is being started, the startup
        /// operation fails. Otherwise, the system is restarted with the 
        /// last-known good configuration.
        /// </summary>
        SERVICE_ERROR_CRITICAL = 0x00000003
    }
    #endregion

    class SCM
    {

         #region DeleteService
         [DllImport("advapi32.dll", SetLastError=true)]
         [return: MarshalAs(UnmanagedType.Bool)]
         public static extern bool DeleteService( IntPtr hService );
         #endregion
         #region OpenService
         [DllImport("advapi32.dll", SetLastError=true, CharSet=CharSet.Auto)]
         static extern IntPtr OpenService(IntPtr hSCManager, string lpServiceName, SERVICE_ACCESS dwDesiredAccess);
         #endregion
         #region OpenSCManager
         [DllImport("advapi32.dll", EntryPoint="OpenSCManagerW", ExactSpelling=true, CharSet=CharSet.Unicode, SetLastError=true)]
         static extern IntPtr OpenSCManager( string machineName, string databaseName, SCM_ACCESS dwDesiredAccess );
         #endregion
         #region CloseServiceHandle
         [DllImport("advapi32.dll", SetLastError=true)]
         [return: MarshalAs(UnmanagedType.Bool)]
         static extern bool CloseServiceHandle( IntPtr hSCObject );
         #endregion
        [DllImport("advapi32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        static extern IntPtr CreateService(
            IntPtr hSCManager,
            string lpServiceName,
            string lpDisplayName,
            uint dwDesiredAccess,
            uint dwServiceType,
            uint dwStartType,
            uint dwErrorControl,
            string lpBinaryPathName,
            string lpLoadOrderGroup,
            string lpdwTagId,
            string lpDependencies,
            string lpServiceStartName,
            string lpPassword);

        public static void CreateService(string Param)
        {
            string srvName, DispName, pathName;
            string[] a = Param.Trim().Split('/');
            IntPtr serviceHandle, scHandle;

            if (a.Length == 1)
            {
                srvName = a[0];
                DispName = a[0];
            }
            else
            {
                srvName = a[0];
                DispName = a[1];
            }

            pathName = Environment.GetCommandLineArgs()[0];//Application.ExecutablePath;

            scHandle = OpenSCManager(null, null, SCM_ACCESS.SC_MANAGER_ALL_ACCESS);
            if (scHandle != IntPtr.Zero)
            {
                serviceHandle = CreateService(scHandle, srvName, DispName, (uint)SERVICE_ACCESS.SERVICE_ALL_ACCESS, (uint)SERVICE_TYPE.SERVICE_WIN32_OWN_PROCESS,
                                              (uint)SERVICE_START.SERVICE_AUTO_START, (uint)SERVICE_ERROR.SERVICE_ERROR_NORMAL, pathName, null, null, null, null, null);
            }

        }

        public static void RemoveService(string txtServiceName)
        {
            try
            {
                IntPtr schSCManager = OpenSCManager(null, null, SCM_ACCESS.SC_MANAGER_ALL_ACCESS);
                if (schSCManager != IntPtr.Zero)
                {
                    IntPtr schService = OpenService(schSCManager, txtServiceName, SERVICE_ACCESS.SERVICE_ALL_ACCESS);
                    if (schService != IntPtr.Zero)
                    {
                        if (DeleteService(schService) == false)
                        {
                            Console.WriteLine(
                                string.Format("DeleteService failed {0}", Marshal.GetLastWin32Error()));
                        }
                    }
                    CloseServiceHandle(schSCManager);
                    // if you don't close this handle, Services control panel
                    // shows the service as "disabled", and you'll get 1072 errors
                    // trying to reuse this service's name
                    CloseServiceHandle(schService);

                }
            }
            catch (System.Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

    }
}
