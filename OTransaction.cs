/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2021-09-06                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Net;
using System.Net.Sockets;
using System.Net.NetworkInformation;
using System.Diagnostics;
using System.Threading;
using System.Collections.Generic;

using Microsoft.SqlServer.Server;

using Newtonsoft.Json;

using K2host.Data.Delegates;
using K2host.Data.Classes;

namespace Data
{

    public partial class OTransaction
    {

        [SqlTrigger(Name = "GetTransaction", Target = "OTransactionListener", Event = "FOR INSERT")]
        public static void GetTransaction()
        {

            string              TransactionConntection  = @"Context Connection = true;";
            SqlTriggerContext   TransactionContext      = SqlContext.TriggerContext;
            SqlPipe             TransactionPipe         = SqlContext.Pipe;

            if (TransactionContext.TriggerAction == TriggerAction.Insert)
            {

                OTrigger Transaction = OTrigger.Build(
                    TransactionContext,
                    TransactionPipe,
                    TransactionConntection
                );

                if (Transaction != null)
                {

                    Dictionary<string, object> Setup = Transaction.GetSetup();

                    Transaction.Include();

                    bool    IsRemote = (bool)Setup["IsRemote"];
                    string  AppTitle = System.Reflection.Assembly.GetExecutingAssembly().GetName().Name;

                    if ((IsRemote == false && Process.GetProcessesByName(AppTitle).Length > 0) || IsRemote == true)
                    {
                        SendTransaction(
                            Setup,
                            JsonConvert.SerializeObject(
                                Transaction.GetTransactionData(),
                                Formatting.Indented
                            ),
                            new OSendResponse(delegate (object e) {
                                Transaction.Pipe.Send("ODataTriggerListener: " + ((Exception)e).Message);
                                Transaction.Dispose();
                            }),
                            new OSendResponse(delegate (object e) {
                                Transaction.DeleteTransaction();
                                Transaction.Pipe.Send("ODataTriggerListener: " + Transaction.ToString());
                                Transaction.Dispose();
                            })
                        );
                    }


                }

            }

        }

        public static void SendTransaction(Dictionary<string, object> Setup, string Data, OSendResponse OnError, OSendResponse OnSucess)
        {

            IPAddress   RemoteIpAddress = (IPAddress)Setup["IPAddress"];
            int         RemotePort      = (int)Setup["Port"];

            if (RemoteIpAddress.ToString() == "0.0.0.0")
                RemoteIpAddress = OTrigger.GetLocalIPv4(NetworkInterfaceType.Ethernet);

            var t = new Thread(new ParameterizedThreadStart(delegate (object e) {

                Socket Socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

                try
                {
                    Socket.Connect(new IPEndPoint(RemoteIpAddress, RemotePort));
                    Socket.Send(System.Text.Encoding.ASCII.GetBytes((string)e));
                    Socket.Disconnect(false);
                    Socket.Close();
                    OnSucess(e);
                }
                catch (Exception ex)
                {
                    OnError(ex);
                }
                finally
                {
                    Socket.Dispose();
                }

            }));

            t.Start(Data);

        }

    }

}
