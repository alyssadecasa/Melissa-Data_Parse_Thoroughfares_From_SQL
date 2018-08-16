using System;
using System.Configuration;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;

namespace ModifySQLTable
{
    class UA_Main
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
            string query = "SELECT RecordID, " + thoroughfareCol + " FROM " + ReadTableName + " WHERE RecordID BETWEEN @StartRec AND @EndRec";

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
                        int recordId = Int32.Parse(reader["RecordId"].ToString().Trim());
                        string streetName = reader[thoroughfareCol].ToString().Trim();
                        streetNameRecords.Add(recordId, new ParsedStreetNameRecord("$" + streetName + "$"));
                    }
                }
            }
        }

        public static SqlConnectionStringBuilder BuildConnectionString(SqlConnectionStringBuilder connString)
        {
            connString.DataSource = "";              // Server
            connString.InitialCatalog = "";  // Database
            connString.IntegratedSecurity = true;           // Connection type: Integrated Security

            return connString;
        }

        public void ParseStreetRecords(IDictionary<int, ParsedStreetNameRecord> records)
        {
            bool parsing = true;

            string[] TLTs =
            {
                "$Автодорога ", "$Алея ", "$Аллея ", "$Бульвар ", "$бульв. ", "$бульв ", "$Б-р. ", "$Б-р ", "$В'їзд ", "$дорожній ", "$дор. ",
                "$дор ", "$Дорога ", "$квартал ", "$кв. ", "$кв ", "$вулиця ", "$вул. ", "$вул ", "$Лінія ", "$Набережна ", "$Набережная ",
                "$наб. ", "$наб ", "$площа ", "$пл. ", "$пл ", "$провулок ", "$пров. ", "$пров ", "$проспект ", "$просп. ", "$просп ", "$прос. ",
                "$прос ", "$пр. ", "$пр ", "$пр-т. ", "$пр-т ", "$Проїзд ", "$Тупик ", "$Узвіз ", "$шосе ", "$Aleya ", "$Avtodoroha ", "$Avtodoroga ",
                "$bulʹvar ", "$bulv. ", "$bulv ", "$b-p. ", "$b-p ", "$Doroha ", "$dorozhniy ", "$dor. ", "$dor ", "$kvartal ", "$khutir ", "$kv. ",
                "$kv ", "$Liniya ", "$liniiam ", "$maidan ", "$mistechko ", "$m-r ", "$Naberezhna ", "$Naberezhnaya ", "$nab. ", "$nab ", "$ostriv",
                "$ploshcha ", "$pl. ", "$pl ", "$prospekt ", "$prosp. ", "$prosp ", "$pros. ", "$pros ", "$pr. ", "$pr ", "$pr-t. ", "$pr-t ",
                "$provulok ", "$prov. ", "$prov ", "$Proizd ", "$Proezd ", "$Tupyk ", "$Tupik ", "$tup. ", "$tup ", "$shose ", "$Uzviz ", "$V'izd ",
                "$vulytsya ", "$vulytsia ", "$vul. ", "$vul ", "$бул. ", "$бул ", "$Линия ", "$лин. ", "$лин ", "$Переулок ", "$пер. ", "$пер ",
                "$п. ", "$п ", "$п-к. ", "$п-к ", "$Площадь ", "$пл. ", "$пл ", "$Проспект ", "$Спуск ", "$Улица ", "$ул. ", "$ул ", "$Шоссе ",
                "$шлях ", "$ш. ", "$ш ", "$Alleya ", "$Aleia ", "$Bulvar ", "$bul. ", "$bul ", "$bul'v. ", "$bul'v ", "$Doroga ", "$lin. ", "$lin ",
                "$Pereulok ", "$per. ", "$per ", "$p. ", "$p ", "$Ploshchad ", "$pl. ", "$pl ", "$Prospect ", "$Prospekt ", "$Shosse ", "$sh. ",
                "$sh ", "$Spusk ", "$shliakh ", "$Ulica ", "$Ulitsa ", "$ul. ", "$ul ", "$V'yezd ", "$vNizd ", "$урочище ", "$urochyshche ", "$uzviz "
             };

            string[] TTTs =
            {
                " Автодорога$", " Алея$", " Аллея$", " Бульвар$", " бульв.$", " бульв$", " Б-р.$", " Б-р$", " В'їзд$", " дорожній$", " дор.$",
                " дор$", " Дорога$", " квартал$", " кв.$", " кв$", " вулиця$", " вул.$", " вул$", " Лінія$", " Набережна$", " Набережная$",
                " наб.$", " наб$", " площа$", " пл.$", " пл$", " провулок$", " пров.$", " пров$", " проспект$", " просп.$", " просп$", " прос.$",
                " прос$", " пр.$", " пр$", " пр-т.$", " пр-т$", " Проїзд$", " Тупик$", " Узвіз$", " шосе$", " Aleya$", " Avtodoroha$", " Avtodoroga$",
                " bulʹvar$", " bulv.$", " bulv$", " b-p.$", " b-p$", " Doroha$", " dorozhniy$", " dor.$", " dor$", " kvartal$", " khutir$", " kv.$",
                " kv$", " Liniya$", " liniiam$", " maidan$", " mistechko$", " m-r$", " Naberezhna$", " Naberezhnaya$", " nab.$", " nab$", " ostriv",
                " ploshcha$", " pl.$", " pl$", " prospekt$", " prosp.$", " prosp$", " pros.$", " pros$", " pr.$", " pr$", " pr-t.$", " pr-t$",
                " provulok$", " prov.$", " prov$", " Proizd$", " Proezd$", " Tupyk$", " Tupik$", " tup.$", " tup$", " shose$", " Uzviz$", " V'izd$",
                " vulytsya$", " vulytsia$", " vul.$", " vul$", " бул.$", " бул$", " Линия$", " лин.$", " лин$", " Переулок$", " пер.$", " пер$",
                " п.$", " п$", " п-к.$", " п-к$", " Площадь$", " пл.$", " пл$", " Проспект$", " Спуск$", " Улица$", " ул.$", " ул$", " Шоссе$",
                " шлях$", " ш.$", " ш$", " Alleya$", " Aleia$", " Bulvar$", " bul.$", " bul$", " bul'v.$", " bul'v$", " Doroga$", " lin.$", " lin$",
                " Pereulok$", " per.$", " per$", " p.$", " p$", " Ploshchad$", " pl.$", " pl$", " Prospect$", " Prospekt$", " Shosse$", " sh.$",
                " sh$", " Spusk$", " shliakh$", " Ulica$", " Ulitsa$", " ul.$", " ul$", " V'yezd$", " vNizd$", " урочище$", " urochyshche$", " uzviz$"
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
                            pair.Value.Thoro_Leading_T = streetName.Substring(1, tlt.Length - 1).Trim();
                            parsedStreetName = streetName.Substring(tlt.Length);
                            parsing = true;
                            break;
                        }
                    }

                    foreach (string ttt in TTTs)
                    {
                        if (streetName.IndexOf(ttt, StringComparison.OrdinalIgnoreCase) >= 0) // compare ignore case
                        {
                            pair.Value.Thoro_Trailing_T = streetName.Substring(streetName.Length - ttt.Length, ttt.Length - 1).Trim();
                            if (parsing == true) // if thoroughfare has both a tlt and ttt
                            {
                                Console.WriteLine(streetName);
                                Console.WriteLine(parsedStreetName.Length + ", " + ttt.Length);
                                Console.WriteLine(parsedStreetName + ", " + ttt);
                                if (ttt.Length >= parsedStreetName.Length)
                                {
                                    parsedStreetName = String.Empty;
                                }
                                parsedStreetName = parsedStreetName.Substring(0, parsedStreetName.Length - ttt.Length);
                                Console.WriteLine(parsedStreetName);
                            }
                            else
                            {
                                parsedStreetName = streetName.Substring(0, streetName.Length - ttt.Length);
                            }
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
                    " WHERE recordID = @RecordId";
                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.Add("@RecordId", SqlDbType.BigInt);
                        command.Parameters.Add("@ThoroughfareName", SqlDbType.NVarChar);
                        command.Parameters.Add("@Thoro_Lead_Type", SqlDbType.NVarChar);
                        command.Parameters.Add("@Thoro_Trail_Type", SqlDbType.NVarChar);

                        command.Parameters["@RecordId"].Value = thisPair.Key;
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
            // Set Table-specific parameters
            string readTableName = "";  // set  table name
            int startLine = 1, endLine = 8950390;

            thoroughfareCol = "Thoroughfare";
            thoroughfareNameCol = "ThoroughfareName";
            thoroughfareLeadingTypeCol = "ThoroughfareLeadingType";
            thoroughfareTrailingTypeCol = "ThoroughfareTrailingType";

            UA_Main obj = new UA_Main();
            obj.ReadInStreets(readTableName, startLine, endLine);
            obj.ParseStreetRecords(streetNameRecords);
            obj.UpdateSQL(readTableName);
            //DEBUG: print out each key value pair
             foreach (KeyValuePair<int, ParsedStreetNameRecord> pair in streetNameRecords)
            {
                Console.OutputEncoding = Encoding.Unicode;
                Console.WriteLine("Key = {0}, Value = {1}", pair.Key, pair.Value);
            } 

            // Keep console open
            Console.WriteLine("Program finished executing successfully.");
            Console.Read();
        }
    }
}
