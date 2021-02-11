using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.ComponentModel;
using System.Configuration;
using System.ServiceProcess;

namespace sms_mysql.NET
{
    [RunInstaller(true)]
    class smsNETInstaller : System.Configuration.Install.Installer
    {
        public smsNETInstaller()
        {
            ServiceProcessInstaller process = new ServiceProcessInstaller();
            process.Account = ServiceAccount.LocalSystem;

            ServiceInstaller serviceAdmin = new ServiceInstaller();
            serviceAdmin.StartType = ServiceStartMode.Automatic;
            serviceAdmin.ServiceName = "sms_mysql.NET";
            serviceAdmin.DisplayName = "sms_mysql.NET Service";
        
            // добавим созданные инсталлеры
            // к нашему контейнеру
            Installers.Add(process);
            Installers.Add(serviceAdmin);
        }
    }
}
