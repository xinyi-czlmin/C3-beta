using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExtractX
{
    class ExtractObj
    {
        //Fields
        public string Schema { get; set; }
        public string ObjName { get; set; }
        public string FileName { get; set; }
        public string Database { get; set; }
        public string Server { get; set; }
        public string OutputFile { get; set; }
        public string FileExtension { get; set; }
        public string Delimiter { get; set; }
        public char HasHeader { get; set; }

        //Constructor
        public ExtractObj(string _schema, string _objName, string _fileName, string _database
            , string _server, string _outputFile, string _fileExtension, string _delimiter
            ,char _hasHeader)
        {
            Schema = _schema;
            ObjName = _objName;
            FileName = _fileName;
            Database = _database;
            Server = _server;
            OutputFile = _outputFile;
            FileExtension = _fileExtension;
            Delimiter = _delimiter;
            HasHeader = _hasHeader;
        }

        public SqlConnection createConnection()
        {
            string connectionString = "Data Source="+ Server + ";Initial Catalog="+ Database + ";" +
            "Integrated Security=True;Asynchronous Processing=true;";

            SqlConnection con = new SqlConnection(connectionString);

            return con;
        }

        public void WriteToTextFile(long _maxFileSize)
        {
            SqlConnection connection = createConnection();
            SqlCommand command = new SqlCommand("SELECT * FROM "+ Schema + "."+ ObjName, connection);
            connection.Open();
            SqlDataReader datareader = command.ExecuteReader();
     
            int ColumnCount = datareader.FieldCount;

            //Delimiter passed in as string get the actual character
            char delimiter = getDelimiter();

            string ListOfColumns = string.Empty;


            //Used a part of naming convention in file when splitting
            int j = 1;

            //Creates string used for full file path name
            string fullPath = OutputFile + "\\" + FileName + "_" + DateTime.Now.ToString("yyyyMMddHHmmssFFF") + "_" + j.ToString() + "." + FileExtension;

            FileInfo file = new FileInfo(fullPath);
            FileStream fs = file.Create();
            fs.Close();

            //Add Header Record as default unless F is specified
            if (HasHeader == 'F')
            {

            }
            else
            {
                for (int i = 0; i < datareader.FieldCount; i++)
                {
                    ListOfColumns = ListOfColumns + datareader.GetName(i) + delimiter;
                }
                ListOfColumns = ListOfColumns + System.Environment.NewLine;
                //Write the Header record to the file
                File.AppendAllText(fullPath, ListOfColumns);
                ListOfColumns = string.Empty;
            }

            //write the data
            while (datareader.Read())
            {
                ListOfColumns = string.Empty;
                for (int i = 0; i <= ColumnCount - 1; i++)
                {
                    ListOfColumns = ListOfColumns + datareader[i].ToString() + delimiter;
                }

                //Before writing the new line get the size of the file
                fs = File.Open(fullPath, FileMode.Open);
                long filesSize = fs.Length;
                fs.Close();

                //Add a carriage return and line feed
                ListOfColumns = ListOfColumns + System.Environment.NewLine;

                //Get the size of the new row
                long rowSize = System.Text.ASCIIEncoding.Unicode.GetByteCount(ListOfColumns);

                //If when we add the next row of data in bytes to the end of the current file
                //if the new size of the file will be greater than our file size restriction
                //we will create a new file.
                if ((rowSize + filesSize) > _maxFileSize)
                {

                    string row = ListOfColumns;
                    //Create new file
                    j++;
                    fullPath = OutputFile + "\\" + FileName + "_" + DateTime.Now.ToString("yyyyMMddHHmmssFFF") + "_" + j.ToString() + "." + FileExtension;
                    file = new FileInfo(fullPath);
                    fs = file.Create();
                    fs.Close();

                    //Add header Record if needed
                    if (HasHeader == 'F')
                    {

                    }
                    else
                    {
                        for (int i = 0; i < datareader.FieldCount; i++)
                        {
                            ListOfColumns = ListOfColumns + datareader.GetName(i) + delimiter;
                        }
                        ListOfColumns = ListOfColumns + System.Environment.NewLine;
                        //Write the Header record to the file
                        File.AppendAllText(fullPath, ListOfColumns);
                        ListOfColumns = string.Empty;
                    }
                    //Add the row to the file that was going to overflow the previous file
                    File.AppendAllText(fullPath, row);
                } else //Continues to write to existing file
                {
                    File.AppendAllText(fullPath, ListOfColumns);
                }

            }

        }

        private char getDelimiter()
        {
            char delimiter = '\0';
            switch (Delimiter)
            {
                case "Pipe":
                    delimiter = '|';
                    break;
                case "Tab":
                    delimiter = '\t';
                    break;
            }

            //TODO Break if delimiter not set
            if(delimiter== '\0')
            {
                throw new ArgumentException("Delimiter not recognized. Exit");
            }
            return delimiter;
        }

        public void WriteTextFileFixedSize(int size)
        {
            //Creates a working table that stores a rowkey value for each row in the table
            SqlConnection connection = createConnection();
            SqlCommand command = new SqlCommand("if object_id('_tmpTable') is not null drop table _tmpTable;SELECT identity(int, 1, 1) as ID, * into dbo._tmpTable FROM " + Schema + "." + ObjName + ";SELECT COUNT(ID) as Count from dbo._tmpTable", connection);
            connection.Open();

            string query = "";

            //Used a part of naming convention in file when splitting
            int j = 1;

            //Creates string used for full file path name
            string fullPath = OutputFile + "\\" + ObjName + "-" + DateTime.Now.ToString("yyyyMMddHHmmssFFF") + "_" + j.ToString() + ".csv";
            
            //row count while be the maximum number of iterations the loop will do
            int rowCount = (int)command.ExecuteScalar();

            if (connection.State != ConnectionState.Closed)
                connection.Close();


            SqlDataAdapter adp = new SqlDataAdapter();
            DataTable dt = new DataTable();

            FileInfo file = new FileInfo(fullPath);
            FileStream fs = file.Create();

            int i = 1;
            while (i <= rowCount)
            {

                //Put the results of this query into a SQLDataAdapter
                query = "SELECT * from _tmpTable WHERE ID between " + i + " and " + (int)(i + 199999); //May need to change this in future for better memory management
                command.CommandText = query;
                adp.SelectCommand = command;

                try
                {
                    adp.Fill(dt);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }

                //Remove identity column from data table does not need to be written into file
                dt.Columns.RemoveAt(0);

                StringBuilder sb = new StringBuilder();
                StringBuilder subSb = new StringBuilder();

                IEnumerable<string> columnNames = dt.Columns.Cast<DataColumn>().
                                                  Select(column => column.ColumnName);

                //TODO make dynamic delimiter, this is for the header record?
                sb.AppendLine(string.Join("|", columnNames));

                file.Refresh();

                long fileSize = 0;
                fs.Close();

                string rowData = "";

                //Begin to write rows to the file
                foreach (DataRow row in dt.Rows)
                {
                    //Turns a row of data into a string and splits it by the delimiter
                    IEnumerable<string> fields = row.ItemArray.Select(field => field.ToString());
                    rowData = string.Join("|", fields);

                    //Turn string row of data into an array of bytes
                    byte[] bytes = Encoding.ASCII.GetBytes(rowData);

                    //If when we add the next row of data in bytes to the end of the current file
                    //if the new size of the file will be greater than our file size restriction
                    //we will create a new file.
                    if ((bytes.Length + fileSize) > size)
                    {

                        sb.Append(subSb.ToString());
                        File.AppendAllText(fullPath, sb.ToString());
                        sb.Clear();
                        j++;
                        fullPath = OutputFile + "\\" + ObjName + "-" + DateTime.Now.ToString("yyyyMMddHHmmssFFF") + "_" + j.ToString() + ".csv";

                        fileSize = 0;
                        fs.Close();
                        subSb.Clear();
                    }
                    fileSize = fileSize + bytes.Length;
                    subSb.AppendLine(rowData);
                }

                sb.Append(subSb.ToString());
                File.AppendAllText(fullPath, sb.ToString());
                subSb.Clear();
                sb.Clear();

                dt.Clear();

                // increase the iterator to the next 500,000 records;
                i = i + 200000;
            }
        }

    }
}
