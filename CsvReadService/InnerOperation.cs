using CsvReadService.Model;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvReadService
{
    public class InnerOperation
    {
        public InnerOperation()
        {
            WriteToFile("Servis Çalışmaya Başladı. [Tarih: " + DateTime.Now + "]");
            string filepath = "C:\\";
            string fileName = "DreamsSegment.csv";
            bool status = FileControl(filepath, fileName);
            if (status)
            {
                string Path = filepath + "\\" + fileName;
                string newPath = filepath + "\\" + String.Format("{0:d/M/yy}", DateTime.Now) + "_ProcessDone_" + fileName;
                FileNameChange(Path, newPath);
            }
            WriteToFile("Servis Çalışması Bitti. [Tarih: " + DateTime.Now + "]");
        }

        #region FileControl => Dosya kontrol işlemi yapılmaktadır.
        public bool FileControl(string filepath, string fileName)
        {
            try
            {
                bool status = false;
                if (File.Exists(filepath + "\\" + fileName))
                {
                    DataTable table = TransferSqlFromCsv(filepath + "\\" + fileName);
                    status = BulkInsert(table, "SegmentBulkTable");
                    if (status)
                    {
                        List<SegmentVM> listSeg = SegmentList();
                        if (listSeg.Count > 0)
                        {
                            foreach (var sg in listSeg)
                            {
                                string controlStatus = RegistryControl(sg.TcNo);
                                if (controlStatus == "var")
                                {
                                    DataUpdate(sg);
                                }
                                else if (controlStatus == "yok")
                                {
                                    DataInsert(sg);
                                }
                                else if (controlStatus == "hata")
                                {
                                    WriteToFile("FileControl (Hata. TCNO: " + sg.TcNo + ") [Tarih: " + DateTime.Now + "]");
                                }
                            }
                            TempDeleteTableAllRows();
                        }

                    }
                }
                return status;
            }
            catch (Exception ex)
            {
                WriteToFile("FileControl (" + ex.Message + ") [Tarih: " + DateTime.Now + "]");
                return false;
            }
        }
        #endregion

        #region FileNameChange => Dosya adını değiştirmektedir.
        public void FileNameChange(string oldFilePath, string newFilePath)
        {
            try
            {
                FileInfo info = new FileInfo(oldFilePath);
                info.MoveTo(String.Format(newFilePath));
            }
            catch (Exception ex)
            {
                WriteToFile("FileNameChange (" + ex.Message + ") [Tarih: " + DateTime.Now + "]");
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

        #region SegmentList => Geçici dosyada bulunan kayıtları liste olarak alınmaktadır.
        public List<SegmentVM> SegmentList()
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand("SegmentTableList", connection))
                    {
                        List<SegmentVM> list = new List<Model.SegmentVM>();
                        cmd.CommandType = CommandType.StoredProcedure;
                        connection.Open();
                        SqlDataReader dr = cmd.ExecuteReader();
                        while (dr.Read())
                        {
                            list.Add(new SegmentVM { TcNo = dr["TC_KIMLIK_NO"].ToString(), Oncelik = Convert.ToInt32(dr["Oncelik"]), SegmentName = dr["SEGMENT"].ToString() });
                        }
                        connection.Close();
                        return list;
                    }
                }
            }
            catch (Exception ex)
            {
                WriteToFile("SegmentList (" + ex.Message + ") [Tarih: " + DateTime.Now + "]");
                return new List<SegmentVM>();
            }
        }
        #endregion

        #region RegistryControl => Kayıt varmı, yokmu diye kontrol etmektedir. 
        public string RegistryControl(string tcNo)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand("Select COUNT(*) From SegmentTempData Where TcNo = '" + tcNo + "'", connection))
                    {
                        int _parameter = -1;
                        connection.Open();
                        _parameter = Convert.ToInt32(cmd.ExecuteScalar());
                        connection.Close();
                        if (_parameter == 0)
                            return "yok";
                        else if (_parameter == -1)
                            return "hata";
                        else
                            return "var";
                    }
                }
            }
            catch (Exception ex)
            {
                WriteToFile("RegistryControl [ TcNo: " + tcNo + " ](" + ex.Message + ") [Tarih: " + DateTime.Now + "]");
                return "hata";
            }
        }
        #endregion

        #region DataInsert = Yeni kayıt eklenmektedir.
        public void DataInsert(SegmentVM data)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand("Insert into SegmentTempData (SegmentName,TcNo,Priority,CreateDate) values ('" + data.SegmentName + "','" + data.TcNo + "','" + data.Oncelik + "','" + DateTime.Now + "')", connection))
                    {
                        connection.Open();
                        int _parameter = cmd.ExecuteNonQuery();
                        connection.Close();
                    }
                }

            }
            catch (Exception ex)
            {
                WriteToFile("DataInsert [ TcNo: " + data.TcNo + " ](" + ex.Message + ") [Tarih: " + DateTime.Now + "]");
            }
        }
        #endregion

        #region DataUpdate = Kayıt güncellemesi yapılmaktadır.
        public void DataUpdate(SegmentVM data)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand("Update SegmentTempData Set Priority = @priority, SegmentName = @segmentName, CreateDate = @createDate", connection))
                    {
                        cmd.Parameters.AddWithValue("@priority", data.Oncelik);
                        cmd.Parameters.AddWithValue("@segmentName", data.SegmentName);
                        cmd.Parameters.AddWithValue("@createDate", DateTime.Now);
                        connection.Open();
                        cmd.ExecuteNonQuery();
                        connection.Close();
                    }
                }

            }
            catch (Exception ex)
            {
                WriteToFile("DataInsert [ TcNo: " + data.TcNo + " ](" + ex.Message + ") [Tarih: " + DateTime.Now + "]");
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
        public static string ConnectionString = "Data Source=172.23.213.201; Initial Catalog=MoovCRM; Integrated Security=false;user id=ikinciyeniuser; password=X3scjS8VCf;";
        #endregion

    }
}