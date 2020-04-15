using CsvReadService.Model;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace CsvReadService
{
    public class InnerOperation
    {
        public InnerOperation()
        {
            ServiceStart();
        }

        #region ServiceStart
        public void ServiceStart()
        {
            WriteToFile("Servis Çalışmaya Başladı. [Tarih: " + DateTime.Now + "]");

            #region Ftp Server Variables
            string FtpFilePath = ""; // Ftp Server Yolu
            string FtpUsername = ""; // Ftp Username
            string FtpPassword = ""; // Ftp Password
            #endregion

            string FileDatetime = FtpFileControl(FtpFilePath, FtpUsername, FtpPassword);
            if (FileDatetime != "")
            {
                #region Ftp sunucuda dosya varsa
                string SaveFilePath = @"C:\SegmentCsv\" + FileDatetime + "_DreamsSegment.csv";
                if (!File.Exists(SaveFilePath))
                {
                    //Dosya indirildikten sonra dosya yolu dönmektedir.
                    SaveFilePath = FtpFileDownload(SaveFilePath, FtpFilePath, FtpUsername, FtpPassword);
                    if (SaveFilePath != "")
                    {
                        if (File.Exists(SaveFilePath))
                        {
                            //Csv dosyasını okuyup DataTable nesnesine atmaktadır.
                            DataTable table = TransferSqlFromCsv(SaveFilePath);
                            if (table.Rows.Count > 0)
                            {
                                //Gecici tabloya segmetler kaydedilmektedir.
                                if (BulkInsert(table, "SegmentBulkTable"))
                                {
                                    DataUpdate();
                                    DataInsert();
                                    TempDeleteTableAllRows();
                                }
                            }
                        }
                    }
                }
                else
                {
                    WriteToFile("Bu dosya zaten indirilmiştir. (" + SaveFilePath + ")");             
                }
                #endregion
            }
            else
            {
                WriteToFile("Ftp Sunucuda ilgili dosya bulunmamaktadır.");
            }
            WriteToFile("Servis Çalışması Bitti. [Tarih: " + DateTime.Now + "]");
            WriteToFile("-----------------------------------------------------------------------");
        }
        #endregion

        #region FtpFileControl => Ftp sunucu içerisinde dosya kontrol işlemi yapılmaktadır.
        public string FtpFileControl(string FtpFilePath, string FtpUsername, string FtpPassword)
        {
            try
            {
                FtpWebRequest request1 = (FtpWebRequest)WebRequest.Create(FtpFilePath);
                request1.Method = WebRequestMethods.Ftp.GetDateTimestamp;
                request1.Credentials = new NetworkCredential(FtpUsername, FtpPassword);
                FtpWebResponse response = (FtpWebResponse)request1.GetResponse();
                string Year = response.LastModified.Year.ToString();
                string Month = response.LastModified.Month.ToString();
                if (Month.Length == 1)
                    Month = "0" + Month;
                string Day = response.LastModified.Day.ToString();
                if (Day.Length == 1)
                    Day = "0" + Day;
                string Hour = response.LastModified.Hour.ToString();
                string Minute = response.LastModified.Minute.ToString();
                string Second = response.LastModified.Second.ToString();
                return Day + "-" + Month + "-" + Year + " " + Hour + "-" + Minute + "-" + Second;
            }
            catch (Exception ex)
            {
                WriteToFile("FtpFileControl (" + ex.Message + ") [Tarih: " + DateTime.Now + "]");
                return "";
            }
        }
        #endregion

        #region FtpFileDownload => Ftp üzerinden dosya indirme
        public string FtpFileDownload(string SaveFilePath, string FtpFilePath, string FtpUsername, string FtpPassword)
        {
            try
            {
                WebClient request = new WebClient();
                request.Credentials = new NetworkCredential(FtpUsername, FtpPassword);
                byte[] veriDosya = request.DownloadData(FtpFilePath);
                FileStream file = File.Create(SaveFilePath);
                file.Write(veriDosya, 0, veriDosya.Length);
                file.Close();
                return SaveFilePath;
            }
            catch (Exception ex)
            {
                WriteToFile("FtpFileDownload (" + ex.Message + ") [Tarih: " + DateTime.Now + "]");
                return "";
            }
        }
        #endregion

        #region TransferSqlFromCsv => Csv dosyasında bulunan kayıtları datatable nesnesine aktarmaktadır.
        public DataTable TransferSqlFromCsv(string filepath)
        {
            try
            {
                var lines = System.IO.File.ReadAllLines(filepath);
                if (lines.Count() > 0)
                {
                    var columns = lines[0].Split(';');
                    var table = new DataTable();
                    foreach (var c in columns)
                        table.Columns.Add(c);

                    for (int i = 1; i < lines.Count() - 1; i++)
                    {
                        var row = lines[i].Split(';');
                        table.Rows.Add(row);
                    }
                    return table;
                }
                else
                {
                    return new DataTable();
                }
            }
            catch (Exception ex)
            {
                WriteToFile("TransferSqlFromCsv (" + ex.Message + ") [Tarih: " + DateTime.Now + "]");
                return new DataTable();
            }

        }
        #endregion

        #region TempDeleteTableAllRows => Gecici tabloda bulunan tüm kayıtlar silinmektedir.
        public void TempDeleteTableAllRows()
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    using (SqlCommand cmdDelete = new SqlCommand("SegmentTableTruncate", connection))
                    {
                        cmdDelete.CommandType = CommandType.StoredProcedure;
                        connection.Open();
                        cmdDelete.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                WriteToFile("TempDeleteTableAllRows (" + ex.Message + ") [Tarih: " + DateTime.Now + "]");
            }
        }
        #endregion

        #region BulkInsert => Datatable nesnesinde bulunan kayıtları belirtilen tablo'ya toplu olarak aktarılmasını yapmaktadır.
        public bool BulkInsert(DataTable dt, string TabloNametoSave)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection())
                {
                    connection.ConnectionString = ConnectionString;
                    connection.Open();
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                    {
                        bulkCopy.BulkCopyTimeout = 600;
                        bulkCopy.DestinationTableName = TabloNametoSave;
                        bulkCopy.WriteToServer(dt);
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                WriteToFile("BulkInsert (" + ex.Message + ") [Tarih: " + DateTime.Now + "]");
                return false;
            }

        }
        #endregion

        #region WriteToFile => Log yazma metodu.
        public void WriteToFile(string Message)
        {
            string path = AppDomain.CurrentDomain.BaseDirectory + "\\Logs";
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            string filepath = AppDomain.CurrentDomain.BaseDirectory + "\\Logs\\ServiceLog_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";
            if (!File.Exists(filepath))
            {
                using (StreamWriter sw = File.CreateText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }

        }
        #endregion

        #region ConnectionString      
        public static string ConnectionString = "Data Source=servername; Initial Catalog=databasename; Integrated Security=false;user id=username; password=password;";
        #endregion

        #region DataInsert => Yeni kayıt eklenmektedir.
        public void DataInsert()
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {

                    using (SqlCommand cmd = new SqlCommand("SegmentInsert", connection))
                    {
                        connection.Open();
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.ExecuteNonQuery();
                        connection.Close();
                    }

                }

            }
            catch (Exception ex)
            {
                WriteToFile("DataInsert (" + ex.Message + ") [Tarih: " + DateTime.Now + "]");
            }
        }
        #endregion
        
        #region DataUpdate => Kayıt güncellemesi yapılmaktadır.
        public void DataUpdate()
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {

                    using (SqlCommand cmd = new SqlCommand("SegmentUpdate", connection))
                    {
                        connection.Open();
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.ExecuteNonQuery();
                        connection.Close();
                    }

                }

            }
            catch (Exception ex)
            {
                WriteToFile("DataUpdate (" + ex.Message + ") [Tarih: " + DateTime.Now + "]");
            }
        }
        #endregion


        

    }
}