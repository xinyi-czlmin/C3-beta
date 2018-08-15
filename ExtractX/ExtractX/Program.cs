using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExtractX
{
    class Program
    {
        static void Main(string[] args)
        {
            string _database = args[0];
            string _server = args[1];
            string _configFile = args[2];
            string _outputFile = args[3];
            //Todo add File size 
            //Todo add lower limit of file size


            //Create a list of
            List<ExtractObj> listExtractObj = new List<ExtractObj>();


            //Read in the Config file
            using (var reader = new StreamReader(_configFile))
            {
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(',');

                    string _schema = values[0];
                    string _objName = values[1];
                    string _fileName = values[2];
                    string _fileExtension = values[3];
                    string _delimiter = values[4];
                    char _hasHeader = Convert.ToChar(values[5]);

                    ExtractObj obj = new ExtractObj(_schema, _objName, _fileName,_database, _server
                        ,_outputFile,_fileExtension, _delimiter, _hasHeader);

                    listExtractObj.Add(obj);
                }
            }


            foreach (ExtractObj item in listExtractObj)
            {
                //Validate before right begins. If the schema of the object has changed compared to the spec.
                //Failure
                
                //Size is entered in bytes
                item.WriteToTextFile(10000);
            }



            //Console.WriteLine(_database +" "+_server+" "+_configFile);
            Console.ReadLine();
        }
    }
}
