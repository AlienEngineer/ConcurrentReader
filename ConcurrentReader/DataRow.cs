using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConcurrentReader
{
    /// <summary>
    /// Holds one row of data
    /// </summary>
    class DataRow
    {
        public DataRow(int index)
        {
            Index = index;
        }

        public int Index { get; private set; }

        public String[] ColumnNames { get; set; }

        public Object[] Values { get; set; }

        public ITuple ToTuple(IConcurrentDataReader reader)
        {
            var dic = new Dictionary<String, Object>();

            for (int i = 0; i < ColumnNames.Length; i++)
            {
                dic.Add(ColumnNames[i], Values[i]);
            }

            return new Tuple(dic, reader);
        }

    }
}
