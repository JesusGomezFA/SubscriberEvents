namespace SubscriberEvents.DataDB
{
    internal class Connection
    {
        public string GetConnectionOracle()
        {
            string GET_CONNECTION_DB_ORACLE = "Data Source = (DESCRIPTION = (ADDRESS_LIST = (" +
                    "ADDRESS = (PROTOCOL = TCP)(HOST = 10.203.100.160)(PORT = 1527)))" +
                    "(CONNECT_DATA =(SERVICE_NAME =  odsdb)));" +
                    "User Id=AUTAXIA;Password=rz$NPj2q!zvg;";
            //string GET_CONNECTION_DB_ORACLE = "Data Source = (DESCRIPTION = (ADDRESS_LIST = (" +
            //        "ADDRESS = (PROTOCOL = TCP)(HOST = 10.203.100.160)(PORT = 1527)))" +
            //        "(CONNECT_DATA =(SERVICE_NAME = odsdb)));" +
            //        "User Id=SQL_JSGOMEZPE2; Password=*!4jSC$2jAbc; ";
            return GET_CONNECTION_DB_ORACLE;
        }
        public string GetConnectionMySql()
        {
            string GET_CONNECTION_DB_SQL = @"Server=10.203.200.31,53100;Database=E2E_MovistarMoney_PROD;User Id=E2E_MovistarMoney;Password=Tele2021*!May#;";
            return GET_CONNECTION_DB_SQL;
        }
    }
}
