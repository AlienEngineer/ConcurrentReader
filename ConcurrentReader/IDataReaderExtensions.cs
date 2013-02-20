using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConcurrentReader
{
    public static class IDataReaderExtensions
    {

        public static ConcurrentDBReader MakeConcurrent(this IDataReader reader)
        {
            return new ConcurrentDBReader(reader);
        }

        public static void ForEach(this ConcurrentDBReader reader, Action<IDataReader> action)
        {
            var ts = new HashSet<Task>();

            for (int i = 0; i < Environment.ProcessorCount; i++)
            {
                ts.Add(Task.Factory.StartNew(() =>
                {
                    while (reader.Read())
                    {
                        action(reader);
                    }
                }));
            }

            reader.Close();

            Task.WaitAll(ts.ToArray());
        }

    }
}
