using Oracle.ManagedDataAccess.Client;
using SubscriberEvents.DataDB;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace SubscriberEvents.Logic
{
    internal class SubscriberEventsLogic
    {

        public static void SubscriberEvents()
        {
            CargarArchivo();
        }

        public static void CargarArchivo()
        {
            //cantidadOrdenes obtiene el conteo total de las ordenes
            int cantidadOrdenes = SetTableHistoricalSql();
            string fechaArchivo = DateTime.Now.ToString("dd-MM-yyyy");
            string horaArchivo = DateTime.Now.ToString("HH-mm");
            if (cantidadOrdenes == 0)
            {
                try
                {
                    //CREAMOS ARCHIVO CSV
                    using (StreamWriter sw = new StreamWriter(@"E:\Documentos\SubscriberEvents_" + fechaArchivo + "_" + horaArchivo + ".csv", false, Encoding.UTF8))
                    {
                        //copiar encabezados de la consulta
                        sw.Write("no se encontraron registros");
                        sw.Write(sw.NewLine); //saltamos linea
                        sw.Close(); // cierra conexion para poder generar envio del archivo
                        SendFile sendFilers = new SendFile();
                        sendFilers.Send(@"E:\Documentos\SubscriberEvents_" + fechaArchivo + "_" + horaArchivo + ".csv");
                    }
                    Console.WriteLine("Informacion Enviada a servidor");
                }
                catch (Exception)
                {
                    ErrorMessage();
                }
            }
            else
            {
                Console.WriteLine("Inicio De proceso");
                if (cantidadOrdenes >= 999)
                {
                    CreateMM_DTSuscriberMV(ShangeState(SetTableConsultOracle()));
                    SendFileServer();
                    Console.WriteLine("Archivo Enviado");
                }
                else
                {
                    CreateMM_DTSuscriberMV(ShangeState(SetTableConsultOracle()));
                    SendFileServer();
                    Console.WriteLine("Archivo Enviado");
                }

            }
        }
        //se asigna la consulta de oracle que se ejecutara en procedimiento batch
        public static DataTable SetTableConsultOracle()
        {
            
            using (SqlConnection conSql = new SqlConnection(GetConnectionSql()))
            {
                using (OracleConnection conOracle = new OracleConnection(GetConnectionOracle()))
                {
                    Console.WriteLine("Inicio De consulta");
                    conOracle.Open();
                    List<string> listaConsulta = new List<string>();
                    string QueryMV;
                    string union;
                    string querySql = "select * from MM_DTMovistarP where ESTADO_ORDEN <> 'Cancelado' ";
                    string queryOracle = "select * from MM_PGeneral where id = '9'";
                    SqlDataAdapter consultaHistorico = new SqlDataAdapter(querySql, conSql);
                    SqlDataAdapter consultaOracle = new SqlDataAdapter(queryOracle, conSql);
                    DataTable dataTableConsulta = new DataTable();
                    DataTable dataTableConsultaOracle = new DataTable();
                    DataTable dataTableOracle = new DataTable();
                    consultaOracle.Fill(dataTableConsultaOracle);
                    consultaHistorico.Fill(dataTableConsulta);
                    for (int i = 0; i <= dataTableConsulta.Rows.Count - 1; i++)
                    {
                        listaConsulta.Add("'" + dataTableConsulta.Rows[i]["NUMERO_CELULAR"].ToString() + "'");
                        if (listaConsulta.Count == 999 || i == dataTableConsulta.Rows.Count - 1)
                        {
                            union = string.Join(",", listaConsulta);
                            QueryMV = dataTableConsultaOracle.Rows[0]["Query"].ToString();
                            QueryMV = QueryMV.Replace("()", "(" + union + ")");
                            OracleCommand oracleCommand = new OracleCommand(QueryMV, conOracle);
                            oracleCommand.CommandType = CommandType.Text;
                            OracleDataReader dr = oracleCommand.ExecuteReader();
                            dataTableOracle.Load(dr);
                            listaConsulta.Clear();
                            QueryMV = "";
                        }
                    }
                    Console.WriteLine("Fin De consulta");
                    conOracle.Close();
                    return dataTableOracle;
                }
            }
        }
        //realiza los cambios de estados posventa
        public static DataTable ShangeState(DataTable dataTableOracle)
        {
            using (SqlConnection sqlConnection = new SqlConnection(GetConnectionSql()))
            {
                Console.WriteLine("Inicio De Cambio De Proceso");
                DataTable dataTableBulkCopySql = new DataTable();
                DataTable dataTableBulkCopy = new DataTable();
                dataTableBulkCopySql = dataTableOracle.Copy();
                dataTableBulkCopySql.Columns[1].MaxLength = 100;
                foreach (DataRow row in dataTableBulkCopySql.Rows)
                {
                    try
                    {
                        SqlDataAdapter queryHistorico = new SqlDataAdapter("Select * from MM_HistoricoPosVent where INDICADOR = '" + row[1].ToString() + "'", sqlConnection);
                        dataTableBulkCopy.Clear();
                        queryHistorico.Fill(dataTableBulkCopy);
                        row["POSVENTA"] = dataTableBulkCopy.Rows[0]["DESCRIPCION"].ToString();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("estado sin cambio: " + ex.Message);
                    }
                }
                return dataTableBulkCopySql;
            }

        }
        //realiza copia de la informacion a la tabla de SqlServer
        public static void CreateMM_DTSuscriberMV(DataTable dataTableBulkCopySql)
        {
            using (SqlConnection sqlConnection = new SqlConnection(GetConnectionSql()))
            {
                Console.WriteLine("Inicio Envio de informacion BD");
                sqlConnection.Open();
                DataTable dataBulkCopy = new DataTable();
                dataBulkCopy.Columns.Add(new DataColumn("ORDEN_PV", typeof(string)));
                dataBulkCopy.Columns.Add(new DataColumn("POSVENTA", typeof(string)));
                dataBulkCopy.Columns.Add(new DataColumn("CLIENTE", typeof(string)));
                dataBulkCopy.Columns.Add(new DataColumn("CUENTA", typeof(string)));
                dataBulkCopy.Columns.Add(new DataColumn("TIPO_DOCUMENTO", typeof(string)));
                dataBulkCopy.Columns.Add(new DataColumn("DOCUMENTO", typeof(string)));
                dataBulkCopy.Columns.Add(new DataColumn("NUMERO_CELULAR", typeof(string)));
                dataBulkCopy.Columns.Add(new DataColumn("NUMERO_CELULAR1", typeof(string)));
                dataBulkCopy.Columns.Add(new DataColumn("NUEVO_CICLO", typeof(string)));
                dataBulkCopy.Columns.Add(new DataColumn("FECHA_EJECUCION", typeof(string)));
                DataRow row;
                foreach (DataRow item in dataTableBulkCopySql.Rows)
                {
                    // Tratamiento de la fecha quitando pm y am
                    //DateTime fecha_ejecucion = Convert.ToDateTime(x, CultureInfo.CurrentCulture);
                    string numero_baja = item[6].ToString();
                    row = dataBulkCopy.NewRow();
                    row[0] = item[0].ToString();
                    row[1] = item[1].ToString();
                    row[2] = item[2].ToString();
                    row[3] = item[3].ToString();
                    row[4] = item[4].ToString();
                    row[5] = item[5].ToString();
                    switch (item[6].ToString().Length)
                    {
                        case 10:
                            row[6] = item[6].ToString();
                            break;
                        case 12:
                            row[6] = item[6].ToString().Substring(2);
                            break;
                    }
                    row[7] = item[7].ToString();
                    row[8] = item[8].ToString();
                    if (!string.IsNullOrEmpty(item[9].ToString()))
                    {
                        row[9] = Convert.ToDateTime(item[9], CultureInfo.CurrentCulture);
                    }
                    dataBulkCopy.Rows.Add(row);
                }
                DataRow[] rowArray = dataBulkCopy.Select();
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConnection))
                {
                    bulkCopy.DestinationTableName = "dbo.MM_DTSuscriberMV";
                    try
                    {
                        //Se copia las columnas de consulta a nuestra base de datos.
                        bulkCopy.WriteToServer(rowArray);
                    }
                    catch (Exception)
                    {
                        ErrorMessage();
                    }
                }
                sqlConnection.Close();
            }
        }
        //Envia datos a servidor remoto
        public static void SendFileServer()
        {
            using (SqlConnection sqlConnection = new SqlConnection(GetConnectionSql()))
            {
                Console.WriteLine("Enviando Archivo");
                int cantidadColumnas;
                DateTime fecha = DateTime.Now;
                string fechaArchivo = DateTime.Now.ToString("dd-MM-yyyy");
                string horaArchivo = DateTime.Now.ToString("HH-mm");
                string fechaMenos = fecha.AddDays(-1).ToString("d/MM/yyyy");
                SqlDataAdapter consultaMovistarMoney = new SqlDataAdapter("SELECT p.OrderID, s.POSTVENTA, s.CLIENTE, s.CUENTA, s.TIPO_DOCUMENTO, s.DOCUMENTO, p.NUMERO_CELULAR, s.NUEVO_NUMERO, s.NUEVO_CICLO, s.FECHA_EJECUCION FROM MM_DTSuscriberMV s INNER JOIN MM_DTMovistarP p ON s.CUENTA = p.CUENTA  OR S.NUMERO_CELULAR = P.NUMERO_CELULAR where s.FECHA_EJECUCION LIKE '%" + fechaMenos + "%' AND p.ESTADO_ORDEN <> 'Cancelado'", sqlConnection);
                DataTable MovistarMoney = new DataTable();
                consultaMovistarMoney.Fill(MovistarMoney);
                try
                {
                    //StreamWriter servidorWrite = new StreamWriter(@"C:\Users\jsgomezpe2\Desktop\Trabajo Celula Axia\OneDrive - fractalia.es\archivos prueba\subscriber\SubscriberEvents_" + fechaArchivo + "_" + horaArchivo + ".csv", false, Encoding.UTF8)
                    //StreamWriter servidorWrite = new StreamWriter(@"E:\Documentos\SubscriberEvents_" + fechaArchivo + "_" + horaArchivo + ".csv", false, Encoding.UTF8)
                    using(StreamWriter servidorWrite = new StreamWriter(@"E:\Documentos\SubscriberEvents_" + fechaArchivo + "_" + horaArchivo + ".csv", false, Encoding.UTF8))
                    {
                        cantidadColumnas = MovistarMoney.Columns.Count;
                        for (int ncolumna = 0; ncolumna < cantidadColumnas; ncolumna++)
                        {
                            servidorWrite.Write(MovistarMoney.Columns[ncolumna]);
                            if (ncolumna < cantidadColumnas - 1)
                            {
                                servidorWrite.Write("|");
                            }
                        }
                        servidorWrite.Write(servidorWrite.NewLine); //saltamos linea
                        foreach (DataRow renglon in MovistarMoney.Rows)
                        {
                            for (int ncolumna = 0; ncolumna < cantidadColumnas; ncolumna++)
                            {
                                if (!Convert.IsDBNull(renglon[ncolumna]))
                                {
                                    servidorWrite.Write(renglon[ncolumna]);
                                }
                                if (ncolumna < cantidadColumnas)
                                {
                                    servidorWrite.Write("|");
                                }
                            }
                            servidorWrite.Write(servidorWrite.NewLine); //saltamos linea
                        }
                        servidorWrite.Close();
                        SendFile sendFilers = new SendFile();
                        sendFilers.Send(@"E:\Documentos\SubscriberEvents_" + fechaArchivo + "_" + horaArchivo + ".csv");
                        //sendFilers.Send(@"C:\Users\jsgomezpe2\Desktop\Trabajo Celula Axia\OneDrive - fractalia.es\archivos prueba\subscriber\SubscriberEvents_" + fechaArchivo + "_" + horaArchivo + ".csv");
                    }
                    Console.WriteLine("Informacion Enviada a servidor");


                }
                catch (Exception)
                {
                    ErrorMessage();
                }

            }
        }
        //obtiene la cantidad datos de la consulta realizada a Sql
        public static int SetTableHistoricalSql()
        {
            using (SqlConnection conSql = new SqlConnection(GetConnectionSql()))
            {
                conSql.Open();
                string query = "select * from MM_DTMovistarP where ESTADO_ORDEN <> 'Cancelado'";
                SqlDataAdapter Historico = new SqlDataAdapter(query, conSql);
                DataTable DbHistorical = new DataTable();
                Historico.Fill(DbHistorical);
                return DbHistorical.Rows.Count;
            }
        }
        //metodo para realizar conexion con base de datos de SQL
        public static string GetConnectionSql()
        {
            Connection connection = new Connection();
            try
            {
                SqlConnection connectionSql = new SqlConnection();
                return connectionSql.ConnectionString = connection.GetConnectionMySql(); ;
            }
            catch (Exception ex)
            {
                
                Console.WriteLine("Error en conexion con SQL");
                SqlConnection connectionSql = new SqlConnection();
                connectionSql.Open();
                string insert = $"insert into MM_Log(Fecha,Problema,Consola) " +
                    $"values ('{FechaArchivo()}','problema al conectar con base de datos SQL','FileUpload')";
                SqlCommand comando = new SqlCommand(insert, connectionSql);
                comando.ExecuteNonQuery();
                connectionSql.Close();
                Console.WriteLine("error: " + ex.Message);
                return null;
            }
        }
        //metodo para realizar conexion con base de datos Oracle
        public static string GetConnectionOracle()
        {
            
            try
            {
                Connection connection = new Connection();
                OracleConnection connectionOracle = new OracleConnection();
                return connectionOracle.ConnectionString = connection.GetConnectionOracle(); ;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error en conexion con oracle");
                using (SqlConnection cn = new SqlConnection(GetConnectionSql()))
                {
                    cn.Open();
                    string insert = $"insert into MM_Log(Fecha,Problema,Consola) values ('{FechaArchivo()}','problema al conectar con base de datos ORACLE','FileUpload')";
                    SqlCommand comando = new SqlCommand(insert, cn);
                    comando.ExecuteNonQuery();
                    cn.Close();
                }
                Console.WriteLine("error: " + ex.Message);
                return null;

            }

        }
        // el metodo agrega la fecha actual con la hora para los mensajes de error
        public static string FechaArchivo()
        {
            DateTime fechaArchivo = DateTime.Now;
            string fecha_menos = fechaArchivo.AddDays(-1).ToString("dd/MM/yyyy");
            return fecha_menos;
        }
        public static void ErrorMessage()
        {
            using (SqlConnection con = new SqlConnection(GetConnectionSql()))
            {
                string FechaArchivo = DateTime.Now.ToString("dd-MM-yyyy");
                string horaArchivo = DateTime.Now.ToString("HH-mm");
                string insert = "insert into MM_Log(Fecha,Problema,Consola) values ('" + FechaArchivo + "_" + horaArchivo + "','Error al crear el archivo .csv','SuscriberEvents')";
                SqlCommand comando = new SqlCommand(insert, con);
            }
            Console.WriteLine("Error Enviado a DB");
        }

    }
}
