using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace ConcurrentReader.Tests
{
    class Connection
    {

        public IDataReader ExecuteReader(String query)
        {
            var settings = ConfigurationManager.ConnectionStrings[Environment.MachineName];

            if (settings == null)
            {
                throw new ConfigurationErrorsException("No settings were found for this machine [" + Environment.MachineName + "].");
            }

            var connection = new SqlConnection(settings.ConnectionString);
            {
                connection.Open();

                var command = connection.CreateCommand();
                command.CommandText = query;
                return command.ExecuteReader(CommandBehavior.CloseConnection);
            }
        }

    }
}
