using System;
using System.Collections.Generic;
using System.Text;

namespace ModifySQLTable
{
    class ParsedStreetNameRecord
    {
        // Fields
        public string StreetName { get; set; }
        public string Thoroughfare { get; set; }
        public string Thoro_Leading_T { get; set; }
        public string Thoro_Trailing_T { get; set; }

        // Constructor
        public ParsedStreetNameRecord(string streetName)
        {
            StreetName = streetName;
        }

        public ParsedStreetNameRecord()
        {

        }

        // Override ToString
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("Full Street Name: " + StreetName + "\n");
            sb.Append("TLT:              " + Thoro_Leading_T + "\n");
            sb.Append("Thoroughfare:     " + Thoroughfare + "\n");
            sb.Append("TTT:              " + Thoro_Trailing_T + "\n");

            return sb.ToString();
        }
    }
}
