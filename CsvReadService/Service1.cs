using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace CsvReadService
{
    public partial class Service1 : ServiceBase
    {
        Timer timer = new Timer();
        public Service1()
        {
            InitializeComponent();
        }
        public void onDebug()
        {
            OnStart(null);
        }

        protected override void OnStart(string[] args)
        {
            timer.Elapsed += new ElapsedEventHandler(OnElapsedTime);
            timer.Interval = 1000 * 60 * 2;
            timer.Enabled = true;
        }

        private void OnElapsedTime(object sender, ElapsedEventArgs e)
        {
            InnerOperation operation = new InnerOperation();
        }

        protected override void OnStop()
        {
        }
    }
}
