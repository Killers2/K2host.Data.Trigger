/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2021-09-06                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

using Newtonsoft.Json;
using Microsoft.SqlServer.Server;

namespace K2host.Data.Classes
{

    public class OTrigger : IDisposable
    {

        #region Properties

        public string  RecordId { get; set; }
        public string  Updated { get; set; }
        public string  Action { get; set; }
        public string  DatabaseName { get; set; }
        public string  Tablename { get; set; }
        public string  PkColumnName { get; set; }
        public string  Uid { get; set; }
        public DataSet Data { get; set; }
        public string  ServiceName { get; set; }
        public string  AddressName { get; set; }
        public string AuthenticationKey { get; set; }
        public string ConnectionString { get; set; }
        public SqlTriggerContext Context { get; set; }
        public SqlPipe Pipe { get; set; }

        #endregion

        #region Constuctor

        public OTrigger()
        {
            ConnectionString    = string.Empty;
            RecordId            = string.Empty;
            Tablename           = string.Empty;
            Action              = string.Empty;
            Uid                 = string.Empty;
            Updated             = string.Empty;
            PkColumnName        = string.Empty;
            Data                = new DataSet("ODataTriggerItem");
            ServiceName         = string.Empty;
            AddressName         = string.Empty;
            AuthenticationKey   = string.Empty;

        }

        public OTrigger(string TransactionConntection)
            : this()
        {
            ConnectionString = TransactionConntection;
        }

        #endregion

        #region Methods

        public void Include()
        {
            if (Data == null)
                return;

            this.Data.Tables.Add(this.ToTable());

        }

        public DataSet GetTransactionData()
        {

            DataSet Temp;

            if (Action.ToLower() == "d")
            {
                Temp = JsonConvert.DeserializeObject<DataSet>(
                    Data.Tables[0].Rows[0]["Updated"].ToString()
                );
                Data.Tables[0].Rows[0]["Updated"] = "-1";
            }
            else
            {
                Temp = Get(
                    "ODataTriggerListener_GetTransaction",
                    new SqlParameter[] {
                        CreateParam(DbType.String, Tablename, "@Tablename"),
                        CreateParam(DbType.String, Uid, "@Uid")
                    },
                    ConnectionString
                );
            }

            Temp.Tables[0].TableName = Tablename;

            Data.Tables.Add(Temp.Tables[0].Copy());

            ClearData(Temp);

            return Data;

        }

        public bool DeleteTransaction()
        {

            return Query("DELETE FROM ODataTriggerListener WHERE Uid = " + RecordId, ConnectionString);

        }

        public override string ToString()
        {
            return DatabaseName + ", " + Tablename + ", " + RecordId + ", " + Action + ", " + PkColumnName + ", " + Updated + ", " + Uid + ", " + ServiceName + ", " + AddressName + ", " + AuthenticationKey;
        }

        public DataTable ToTable()
        {
            DataTable dt = new DataTable("Trigger");

            dt.Columns.AddRange(new DataColumn[] {
                new DataColumn("RecordId",          typeof(string)),
                new DataColumn("Updated",           typeof(string)),
                new DataColumn("Action",            typeof(string)),
                new DataColumn("DatabaseName",      typeof(string)),
                new DataColumn("Tablename",         typeof(string)),
                new DataColumn("PkColumnName",      typeof(string)),
                new DataColumn("Uid",               typeof(string)),
                new DataColumn("ServiceName",       typeof(string)),
                new DataColumn("AddressName",       typeof(string)),
                new DataColumn("AuthenticationKey", typeof(string))
            });

            DataRow dr = dt.NewRow();

            dr.ItemArray = new object[]
            {
                RecordId,
                Updated,
                Action,
                DatabaseName,
                Tablename,
                PkColumnName,
                Uid,
                ServiceName,
                AddressName,
                AuthenticationKey
            };

            dt.Rows.Add(dr);

            return dt;

        }

        public Dictionary<string, object> GetSetup()
        {

            Dictionary<string, object> results = new Dictionary<string, object>();

            DataSet SetUp = Get("ODataTriggerListener_GetSetup", null, ConnectionString);

            results.Add("IPAddress",            IPAddress.Parse(SetUp.Tables[0].Rows[0]["RemoteIP"].ToString()));
            results.Add("Port",                 Convert.ToInt32(SetUp.Tables[0].Rows[0]["RemotePort"]));
            results.Add("IsRemote",             Convert.ToBoolean(SetUp.Tables[0].Rows[0]["IsRemote"]));
            results.Add("ServiceName",          SetUp.Tables[0].Rows[0]["Service"].ToString());
            results.Add("AddressName",          SetUp.Tables[0].Rows[0]["Address"].ToString());
            results.Add("AuthenticationKey",    SetUp.Tables[0].Rows[0]["AuthKey"].ToString());

            ServiceName         = results["ServiceName"].ToString();
            AddressName         = results["AddressName"].ToString();
            AuthenticationKey   = results["AuthenticationKey"].ToString();

            ClearData(SetUp);

            return results;

        }

        public static OTrigger Build(SqlTriggerContext context, SqlPipe pipe, string transactionConntection)
        {

            string sqlRedeam = "SELECT * FROM INSERTED;";

            if (context.TriggerAction == TriggerAction.Delete)
                sqlRedeam = "SELECT * FROM DELETED;";

            DataSet e = Get(sqlRedeam, transactionConntection, out string DatabaseName);

            if (e.Tables[0].Rows.Count <= 0)
            {
                ClearData(e);
                return null;
            }

            OTrigger r = new OTrigger(transactionConntection)
            {
                RecordId        = e.Tables[0].Rows[0]["Uid"].ToString(),
                Updated         = e.Tables[0].Rows[0]["Updated"].ToString(),
                Action          = e.Tables[0].Rows[0]["Action"].ToString(),
                DatabaseName    = DatabaseName,
                Tablename       = e.Tables[0].Rows[0]["Table"].ToString(),
                PkColumnName    = e.Tables[0].Rows[0]["PkColumnName"].ToString(),
                Uid             = e.Tables[0].Rows[0]["Rid"].ToString(),
                Context         = context,
                Pipe            = pipe
            };

            ClearData(e);

            return r;

        }

        #endregion

        #region Static Methods and Functions

        public static SqlParameter CreateParam(DbType datatype, object value, string paramname)
        {
            try
            {
                SqlParameter temp = new SqlParameter
                {
                    DbType = datatype,
                    Value = value,
                    ParameterName = paramname
                };
                return temp;
            }
            catch
            {
                return null;
            }
        }

        public static bool Query(string Sql, string ConnectionString, int sqltimout = 500)
        {
            try
            {
                using (var tempconnection = new SqlConnection(ConnectionString))
                using (var tempcommand = new SqlCommand(Sql))
                {
                    bool tempbool = false;

                    tempcommand.CommandTimeout = sqltimout;
                    tempcommand.Connection = tempconnection;

                    tempconnection.Open();

                    if (tempcommand.ExecuteNonQuery() > 0)
                        tempbool = true;

                    return tempbool;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static DataSet Get(string StoredProc, SqlParameter[] Parameters, string ConnectionString)
        {
            try
            {

                using (var tempconnection = new SqlConnection(ConnectionString))
                using (var tempcommand = new SqlCommand(StoredProc))
                using (var tempadaptor = new SqlDataAdapter())
                {
                    tempcommand.Connection = tempconnection;
                    tempcommand.CommandTimeout = 10000;
                    tempcommand.CommandType = CommandType.StoredProcedure;

                    if (Parameters != null)
                        tempcommand.Parameters.AddRange(Parameters);

                    tempadaptor.SelectCommand = tempcommand;

                    var tempdataset = new DataSet();

                    tempadaptor.Fill(tempdataset);

                    return tempdataset;
                }

            }
            catch
            {
                return null;
            }


        }

        public static DataSet Get(string Sql, string ConnectionString)
        {
            try
            {
                using (var tempconnection = new SqlConnection(ConnectionString))
                using (var tempcommand = new SqlCommand(Sql))
                using (var tempadaptor = new SqlDataAdapter())
                {
                    tempcommand.CommandTimeout = 500;
                    tempcommand.Connection = tempconnection;
                    tempadaptor.SelectCommand = tempcommand;

                    var tempdataset = new DataSet();

                    tempadaptor.Fill(tempdataset);

                    return tempdataset;
                }
            }
            catch
            {
                return null;
            }
        }

        public static DataSet Get(string Sql, string ConnectionString, out string DatabaseName)
        {

            DatabaseName = string.Empty;

            try
            {
                using (var a = new SqlConnection(ConnectionString))
                {

                    a.Open();

                    DatabaseName = a.Database;

                    using (var b = new SqlCommand(Sql))
                    {
                        using (var c = new SqlDataAdapter())
                        {
                            b.CommandTimeout = 500;
                            b.Connection = a;
                            c.SelectCommand = b;

                            var d = new DataSet();

                            c.Fill(d);

                            a.Close();

                            return d;

                        }

                    }

                }

            }
            catch
            {
                return null;
            }
        }

        public static bool ClearData(DataSet e)
        {
            try
            {
                e.Clear();
                e.Tables.Clear();
                e.Dispose();
                e = null;
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static IPAddress LocalIPAddress()
        {
            if (!System.Net.NetworkInformation.NetworkInterface.GetIsNetworkAvailable())
                return IPAddress.None;

            return Dns.GetHostEntry(Dns.GetHostName())
                .AddressList
                .FirstOrDefault(ip => ip.AddressFamily == AddressFamily.InterNetwork);
        }

        public static IPAddress GetLocalIPv4(NetworkInterfaceType _type)
        {

            IPAddress output = IPAddress.None;

            foreach (NetworkInterface item in NetworkInterface.GetAllNetworkInterfaces())
            {

                if (item.NetworkInterfaceType == _type && item.OperationalStatus == OperationalStatus.Up)
                {
                    IPInterfaceProperties adapterProperties = item.GetIPProperties();

                    if (adapterProperties.GatewayAddresses.FirstOrDefault() != null)
                        foreach (UnicastIPAddressInformation ip in adapterProperties.UnicastAddresses)
                            if (ip.Address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                            {
                                output = ip.Address;
                                break;
                            }

                }

                if (output != IPAddress.None)
                    break;

            }

            return output;
        }

        #endregion

        #region Destuctor

        private bool IsDisposed = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!IsDisposed)
            {
                if (disposing)
                {
                    ClearData(Data);
                }

            }
            IsDisposed = true;
        }

        public void Dispose()
        {
            // Do not change this code.  Put cleanup code in Dispose(ByVal disposing As Boolean) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

    }

}
