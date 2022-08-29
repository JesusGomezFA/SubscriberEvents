using Renci.SshNet;
using SubscriberEvents.DataDB;
using System;
using System.Data;
using System.Data.SqlClient;

namespace SubscriberEvents.Logic
{
    public class SendFile
    {
        // Ip De conexion
        private  string host = "10.201.135.80";
        // Conexion mediante sshclient
        private  SshClient sshClient;
        //envia el archivo que obtiene de la ruta remota y lo envia al servidor .80
        public  void Send(string fileName)
        {
            string FechaArchivo = DateTime.Now.ToString("dd-MM-yyyy");
            string horaArchivo = DateTime.Now.ToString("HH-mm");
            using (SqlConnection conect = new SqlConnection(GetConnectionSql()))
            {
                conect.Open();
                string Query = "select * from MM_PGeneral where id = '4' ";
                SqlDataAdapter adap = new SqlDataAdapter(Query, conect);
                DataTable tabla = new DataTable();
                adap.Fill(tabla);
                // Nombre de usuario
                string username = Encrypt.Base64_Decode(tabla.Rows[0]["User"].ToString());
                // Contraseña de conexion
                string password = Encrypt.Base64_Decode(tabla.Rows[0]["Password"].ToString());
                KeyboardInteractiveAuthenticationMethod kauth = new KeyboardInteractiveAuthenticationMethod(username);
                PasswordAuthenticationMethod pauth = new PasswordAuthenticationMethod(username, password);
                kauth.AuthenticationPrompt += new EventHandler<Renci.SshNet.Common.AuthenticationPromptEventArgs>(HandleKeyEvent);
                ConnectionInfo connectionInfo = new ConnectionInfo(host, username, pauth, kauth);
                sshClient = new SshClient(connectionInfo);
                sshClient.KeepAliveInterval = TimeSpan.FromSeconds(60);
                try
                {
                    sshClient.Connect();

                }
                catch (Exception)
                {

                    string insert = "insert into MM_Log(Fecha,Problema,Consola) values ('" + FechaArchivo + "_" + horaArchivo + "','Error en las credenciales sftp','SuscriberEvents')";
                    SqlCommand comando = new SqlCommand(insert, conect);
                    comando.ExecuteNonQuery();
                }
                // Upload File
                using (var sftp = new SftpClient(connectionInfo))
                {

                    sftp.Connect();
                    sftp.ChangeDirectory(@"/MoneyAlianza/SubscriberEvents");
                    using (var uplfileStream = System.IO.File.OpenRead(fileName))
                    {
                        sftp.UploadFile(uplfileStream, "SubscriberEvents_" + FechaArchivo + "_" + horaArchivo + ".csv", true);
                    }
                    sftp.Disconnect();
                }
            }
        }
        //decodifica la contraseña y usuario con la key
        public  void HandleKeyEvent(Object sender, Renci.SshNet.Common.AuthenticationPromptEventArgs e)
        {
            using (SqlConnection cn = new SqlConnection(GetConnectionSql()))
            {
                cn.Open();
                string Query = "select * from MM_PGeneral where id = '4' ";
                SqlDataAdapter adap = new SqlDataAdapter(Query, cn);
                DataTable tabla = new DataTable();
                adap.Fill(tabla);
                string password = Encrypt.Base64_Decode(tabla.Rows[0]["Password"].ToString());
                foreach (Renci.SshNet.Common.AuthenticationPrompt prompt in e.Prompts)
                {
                    if (prompt.Request.IndexOf("Password:", StringComparison.InvariantCultureIgnoreCase) != -1)
                    {
                        prompt.Response = password;
                    }
                }

            }
        }
        public  string GetConnectionSql()
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
        public  string FechaArchivo()
        {
            DateTime fechaArchivo = DateTime.Now;
            string fecha_menos = fechaArchivo.AddDays(-1).ToString("dd/MM/yyyy");
            return fecha_menos;
        }
    }
}
