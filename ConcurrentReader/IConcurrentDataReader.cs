using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Threading;

namespace ConcurrentReader
{
    public interface IConcurrentDataReader : IDataReader
    {
        ITuple GetData();
    }
}
