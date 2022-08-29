namespace SubscriberEvents.DataDB
{
    internal class Connection
    {
        public string GetConnectionOracle()
        {
            string GET_CONNECTION_DB_ORACLE = "Data Source = (DESCRIPTION = (ADDRESS_LIST = (" +
                    "ADDRESS = (PROTOCOL = TCP)(HOST = 10.203.100.133)(PORT = 1527)))" +
                    "(CONNECT_DATA =(SERVICE_NAME =  crmdb)));" +
                    "User Id=E2E_SUSPENSION_GESCODE;Password=TEmpoRMoV!!s;";
            //string GET_CONNECTION_DB_ORACLE = "Data Source = (DESCRIPTION = (ADDRESS_LIST = (" +
            //        "ADDRESS = (PROTOCOL = TCP)(HOST = 10.203.100.133)(PORT = 1527)))" +
            //        "(CONNECT_DATA =(SERVICE_NAME =  crmdb)));" +
            //        "User Id=SQL_JSGOMEZPE2;Password=*Ykkuy1996*;";
            return GET_CONNECTION_DB_ORACLE;
        }
        public string GetConnectionMySql()
        {
            string GET_CONNECTION_DB_SQL = @"Server=10.203.200.31,53100;Database=E2E_MovistarMoney_PROD;User Id=E2E_MovistarMoney;Password=Tele2021*!May#;";
            return GET_CONNECTION_DB_SQL;
        }
    }
}
