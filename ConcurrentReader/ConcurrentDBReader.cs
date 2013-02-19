using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;

namespace ConcurrentReader
{
    public class ConcurrentDBReader : IDataReader
    {

        private readonly List<IDictionary<String, Object>> data = new List<IDictionary<String, Object>>();

        private Thread loaderThread;
        private readonly IDataReader _Reader;

        private int current;
        private int running;

        private readonly ConcurrentQueue<Object> lockedThreads = new ConcurrentQueue<Object>();

        private readonly ConcurrentDictionary<Thread, IDictionary<String, Object>> threadAllocatedData = new ConcurrentDictionary<Thread, IDictionary<String, Object>>();

        public ConcurrentDBReader(IDataReader reader)
        {
            _Reader = reader;
            loaderThread = new Thread(LoadingWork);
        }

        private void LoadingWork()
        {
            while (_Reader.Read())
            {
                var row = new Dictionary<String, Object>();
                for (int i = 0; i < _Reader.FieldCount; i++)
                {
                    row[_Reader.GetName(i)] = _Reader[i];
                }

                data.Add(row);

                if (lockedThreads.Count > 0)
                {
                    Object lockObj;
                    lockedThreads.TryDequeue(out lockObj);
                    lock (lockObj)
                    {
                        Monitor.Pulse(lockObj);
                    }
                }
            }
        }

        public void Close()
        {
            throw new NotImplementedException();
        }

        public int Depth
        {
            get { throw new NotImplementedException(); }
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public bool IsClosed
        {
            get { throw new NotImplementedException(); }
        }

        public bool NextResult()
        {
            return Read();
        }

        public bool Read()
        {
            Thread.MemoryBarrier();
            if (running != 1 && Interlocked.CompareExchange(ref running, 1, 0) == 0)
            {
                loaderThread.Start();
            }

            // The reader has catched the loaderThread and must wait
            // or the loaderThread has ended ans there's no more data to read
            if (data.Count == current)
            {
                var lockObj = new Object();
                lockedThreads.Enqueue(lockObj);
                lock (lockObj)
                {
                    Monitor.Wait(lockObj);                    
                }
            }

            var index = Interlocked.Increment(ref current);

            threadAllocatedData[Thread.CurrentThread] = data[index - 1];
            return true;
        }

        public int RecordsAffected
        {
            get { throw new NotImplementedException(); }
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public int FieldCount
        {
            get { throw new NotImplementedException(); }
        }

        #region GETTERS
        public bool GetBoolean(int i)
        {
            throw new NotImplementedException();
        }

        public byte GetByte(int i)
        {
            throw new NotImplementedException();
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            throw new NotImplementedException();
        }

        public decimal GetDecimal(int i)
        {
            throw new NotImplementedException();
        }

        public double GetDouble(int i)
        {
            throw new NotImplementedException();
        }

        public Type GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        public float GetFloat(int i)
        {
            throw new NotImplementedException();
        }

        public Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public short GetInt16(int i)
        {
            throw new NotImplementedException();
        }

        public int GetInt32(int i)
        {
            throw new NotImplementedException();
        }

        public long GetInt64(int i)
        {
            throw new NotImplementedException();
        }

        public string GetName(int i)
        {
            throw new NotImplementedException();
        }

        public int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public string GetString(int i)
        {
            throw new NotImplementedException();
        }

        public object GetValue(int i)
        {
            throw new NotImplementedException();
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        } 
        #endregion

        public bool IsDBNull(int i)
        {
            throw new NotImplementedException();
        }

        public object this[string name]
        {
            get {
                return threadAllocatedData[Thread.CurrentThread][name];
            }
        }

        public object this[int i]
        {
            get { return threadAllocatedData[Thread.CurrentThread].Values.ElementAt(i); }
        }
    }
}
