using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace ConcurrentReader.Tests
{
    class Connection
    {

        public IDataReader ExecuteReader(String query)
        {
            var connection = new SqlConnection(ConfigurationManager.ConnectionStrings[Environment.MachineName].ConnectionString);
            {
                connection.Open();

                var command = connection.CreateCommand();
                command.CommandText = query;
                return command.ExecuteReader(CommandBehavior.CloseConnection);
            }
        }

    }
}
