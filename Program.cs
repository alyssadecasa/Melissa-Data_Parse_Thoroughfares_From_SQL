using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;

namespace ModifySQLTable
{
    class Program
    {
        // Fields
        private static IDictionary<int, ParsedStreetNameRecord> streetNameRecords = new Dictionary<int, ParsedStreetNameRecord>();
        private static String thoroughfareLeadingTypeCol;
        private static String thoroughfareTrailingTypeCol;
        private static String thoroughfareNameCol;
        private static String thoroughfareCol;

        public void ReadInStreets(string ReadTableName, int StartLine, int EndLine)
        {
            // create connection string
            SqlConnectionStringBuilder connString = new SqlConnectionStringBuilder();
            BuildConnectionString(connString);

            // create connection
            SqlConnection connection = new SqlConnection(connString.ToString());

            // get thoroughfare from appsettings
            //string thoroughfare = //todo

            // Set query to be used     //TODO change thoroughfare to property
            string query = "SELECT RecID, " + thoroughfare + " FROM " + ReadTableName + " WHERE RecID BETWEEN @StartRec AND @EndRec";

            using (SqlCommand command = new SqlCommand(query, connection))
            {
                command.Connection.Open();

                // Set parameters to query
                command.Parameters.Add("@StartRec", SqlDbType.Int);
                command.Parameters.Add("@EndRec", SqlDbType.Int);

                command.Parameters["@StartRec"].Value = StartLine;
                command.Parameters["@EndRec"].Value = EndLine;

                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read()) // for each row
                    {
                        int recId = Int32.Parse(reader["RecId"].ToString().Trim());
                        string streetName = reader[thoroughfareCol].ToString().Trim();
                        streetNameRecords.Add(recId, new ParsedStreetNameRecord("$" + streetName + "$"));
                    }
                }
            }
        }

        public static SqlConnectionStringBuilder BuildConnectionString(SqlConnectionStringBuilder connString)
        {
            connString.DataSource = "Brown11";              // Server
            connString.InitialCatalog = "InternWorkspace";  // Database
            connString.IntegratedSecurity = true;           // Connection type: Integrated Security

            return connString;
        }

        public void ParseStreetRecords(IDictionary<int, ParsedStreetNameRecord> records)
        {
            bool parsing = true;

            string[] TLTs =
            {
                "$Brdo ", "$Br. ", "$Br ", "$Breg ", "$Cesta na ", "$Cesta ob ", "$Cesta pod ", "$Cesta v ", "$Cesta ", "$C. ", "$C ", "$Drevored ",
                "$Dvor ", "$Kolonija ", "$Naselje ", "$Park ", "$Pot do ", "$Pot na ", "$Pot v ", "$Pot za ", "$Pot ", "$Steza ", "$Trg ",
                "$Tunel ", "$Ulica v ", "$Ulica za ", "$Ulica ", "$Ul. ", "$Ul "
            };

            string[] TTTs =
            {
                " Aleja$", " Brdo$", " br.$", " br$", " Breg$", " Cesta$", " Drevored$", " Dvor$", " Kolonija$", " Nabrežje$", " Naselje$",
                " Park$", " Pot$", " Steza$", " Trg$", " Tunel$", " Ulica$", " ul.$", " ul$"
            };

            foreach (KeyValuePair<int, ParsedStreetNameRecord> pair in records)
            {
                // Status Message
                if (pair.Key % 10000 == 0)
                {
                    Console.WriteLine("Parsing Key = {0}", pair.Key);
                }

                string streetName = pair.Value.StreetName;
                string parsedStreetName = String.Empty;

                do
                {
                    parsing = false;

                    foreach (string tlt in TLTs)
                    {
                        if (streetName.IndexOf(tlt, StringComparison.OrdinalIgnoreCase) >= 0) // compare ignore case
                        {
                            pair.Value.Thoro_Leading_T = streetName.Substring(1, tlt.Length - 1).Trim(); //tlt.Substring(1).Trim();
                            parsedStreetName = streetName.Substring(tlt.Length);
                            parsing = true;
                            break;
                        }
                    }

                    foreach (string ttt in TTTs)
                    {
                        if (streetName.IndexOf(ttt, StringComparison.OrdinalIgnoreCase) >= 0) // compare ignore case
                        {
                            pair.Value.Thoro_Trailing_T = streetName.Substring(streetName.Length - ttt.Length, ttt.Length - 1).Trim(); // ttt.Substring(0, ttt.Length - 1).Trim();
                            parsedStreetName = streetName.Substring(0, streetName.Length - ttt.Length);
                            parsing = true;
                            break;
                        }
                    }

                    if (parsing)
                    {
                        streetName = parsedStreetName;
                    }
                    pair.Value.Thoroughfare = streetName.Replace("$", "");
                } while (parsing);
            }
        }

        public void UpdateSQL(string updateTableName)
        {
            // create connection string
            SqlConnectionStringBuilder connString = new SqlConnectionStringBuilder();
            BuildConnectionString(connString);

            using (SqlConnection connection = new SqlConnection(connString.ToString()))
            {
                foreach (KeyValuePair<int, ParsedStreetNameRecord> thisPair in streetNameRecords)
                {
                    // Status message
                    if (thisPair.Key % 10000 == 0)
                    {
                        Console.WriteLine("Updating Record for Key = {0}", thisPair.Key);
                    }
                    // TODO - change thoroughfare, thoro_lead_type, etc to properties
                    string query = "UPDATE " + updateTableName + " SET " + thoroughfareNameCol + " = @ThoroughfareName, " +
                       thoroughfareLeadingTypeCol + " = @Thoro_Lead_Type, " + thoroughfareTrailingTypeCol + " = @Thoro_Trail_Type " +
                    " WHERE recID = @RecId";
                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.Add("@RecId", SqlDbType.BigInt);
                        command.Parameters.Add("@ThoroughfareName", SqlDbType.NVarChar);
                        command.Parameters.Add("@Thoro_Lead_Type", SqlDbType.NVarChar);
                        command.Parameters.Add("@Thoro_Trail_Type", SqlDbType.NVarChar);

                        command.Parameters["@RecId"].Value = thisPair.Key;
                        if (!String.IsNullOrEmpty(thisPair.Value.Thoroughfare))
                        {
                            command.Parameters["@ThoroughfareName"].Value = thisPair.Value.Thoroughfare;
                        }
                        else
                        {
                            command.Parameters["@ThoroughfareName"].Value = DBNull.Value;
                        }
                        if (!String.IsNullOrEmpty(thisPair.Value.Thoro_Leading_T))
                        {
                            command.Parameters["@Thoro_Lead_Type"].Value = thisPair.Value.Thoro_Leading_T;
                        }
                        else
                        {
                            command.Parameters["@Thoro_Lead_Type"].Value = DBNull.Value;
                        }
                        if (!String.IsNullOrEmpty(thisPair.Value.Thoro_Trailing_T))
                        {
                            command.Parameters["@Thoro_Trail_Type"].Value = thisPair.Value.Thoro_Trailing_T;
                        }
                        else
                        {
                            command.Parameters["@Thoro_Trail_Type"].Value = DBNull.Value;
                        }
                        command.Connection.Open();
                        command.ExecuteNonQuery();
                        connection.Close();
                    }
                }
            }
        }

        private static void Main(string[] args)
        {
            string readTableName = "InternWorkspace.dbo.SIAddresses_everything_2";
            int startLine = 1, endLine = 556813;

            thoroughfareCol = "Thoroughfare";<DirectedGraph xmlns="http://schemas.microsoft.com/vs/2009/dgml">
  <Nodes>
    <Node Id="(@1 @2)" Visibility="Hidden" />
    <Node Id="(@1 @3 Namespace=299297980;1056828923)" Category="Dependency" Dependency.Id="netcoreapp2.0\NuGetDependency\.NETCoreApp,Version=v2.0\System.Security.Principal.Windows\4.5.0" Dependency.Resolved="True" Icon="259567c1-aa6b-46bf-811c-c145dd9f3b48;9" IsDragSource="True" Label="System.Security.Principal.Windows (4.5.0)" SourceLocation="(Assembly=file:///C:/Users/alyssa/source/repos/ModifySQLTable/ModifySQLTable/ModifySQLTable.csproj StartLineNumber=130 StartCharacterOffset=0 EndLineNumber=130 EndCharacterOffset=0)" />
  </Nodes>
  <Links>
    <Link Source="(@1 @2)" Target="(@1 @3 Namespace=299297980;1056828923)" Category="Contains" />
  </Links>
  <Categories>
    <Category Id="Contains" Label="Contains" Description="Whether the source of the link contains the target object" IsContainment="True" />
    <Category Id="Dependency" />
  </Categories>
  <Properties>
    <Property Id="Dependency.Id" DataType="System.String" />
    <Property Id="Dependency.Resolved" DataType="System.Boolean" />
    <Property Id="Icon" Label="Icon" DataType="System.String" />
    <Property Id="IsContainment" DataType="System.Boolean" />
    <Property Id="IsDragSource" Label="IsDragSource" Description="IsDragSource" DataType="System.Boolean" />
    <Property Id="Label" Label="Label" Description="Displayable label of an Annotatable object" DataType="System.String" />
    <Property Id="SourceLocation" Label="Start Line Number" DataType="Microsoft.VisualStudio.GraphModel.CodeSchema.SourceLocation" />
    <Property Id="Visibility" Label="Visibility" Description="Defines whether a node in the graph is visible or not" DataType="System.Windows.Visibility" />
  </Properties>
  <QualifiedNames>
    <Name Id="Assembly" Label="Assembly" ValueType="Uri" />
    <Name Id="File" Label="File" ValueType="Uri" />
    <Name Id="Namespace" Label="Namespace" ValueType="System.String" />
  </QualifiedNames>
  <IdentifierAliases>
    <Alias n="1" Uri="Assembly=$(VsSolutionUri)/ModifySQLTable/ModifySQLTable.csproj" />
    <Alias n="2" Uri="File=$(VsSolutionUri)/modifysqltable/netcoreapp2.0/nugetdependency/.netcoreapp,version=v2.0/system.data.sqlclient/4.5.1" />
    <Alias n="3" Uri="File=netcoreapp2.0\nugetdependency\.netcoreapp,version=v2.0\system.security.principal.windows\4.5.0" />
  </IdentifierAliases>
  <Paths>
    <Path Id="VsSolutionUri" Value="file:///C:/Users/alyssa/source/repos/ModifySQLTable" />
  </Paths>
</DirectedGraph>
            thoroughfareNameCol = "ThoroughfareName";
            thoroughfareLeadingTypeCol = "ThoroughfareLeadingType";
            thoroughfareTrailingTypeCol = "ThoroughfareTrailingType";

            Program obj = new Program();
            obj.ReadInStreets(readTableName, startLine, endLine);
            obj.ParseStreetRecords(streetNameRecords);
            obj.UpdateSQL(readTableName);
            // DEBUG: print out each key value pair
            // foreach (KeyValuePair<int, ParsedStreetNameRecord> pair in streetNameRecords)
            // {
            //     Console.OutputEncoding = Encoding.Unicode;
            //     Console.WriteLine("Key = {0}, Value = {1}", pair.Key, pair.Value);
            // }

            // Keep console open
            Console.WriteLine("Program finished executing successfully.");
            Console.Read();
        }
    }
}
