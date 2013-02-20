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
        private EventWaitHandle waitHandle = new EventWaitHandle(false, EventResetMode.ManualReset);
        private readonly IDataReader _Reader;

        private int current;
        private int running;

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

            }
            _Reader.Close();
        }

        /// <summary>
        /// Waits until the loading is complete.
        /// </summary>
        public void Close()
        {
            while (loaderThread.ThreadState == ThreadState.Unstarted)
            {
                Thread.Sleep(0);
            }

            loaderThread.Join();
            waitHandle.WaitOne();
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
            // If not running start the loader thread.
            if (Thread.VolatileRead(ref running) != 1 && Interlocked.CompareExchange(ref running, 1, 0) == 0)
            {
                loaderThread.Start();
            }

            // wait while new data is being pushed.
            while (data.Count == Thread.VolatileRead(ref current))
            {
                // If the reading is done while waiting then exit.
                if (_Reader.IsClosed)
                {
                    waitHandle.Set();
                    return false;
                }
                Thread.Sleep(0);
            }

            // moving the cursor to the next position.            
            var index = Interlocked.Increment(ref current) - 1;

            // If more than one thread increments the cursor then it could turn into an invalid index.
            // If the index is not valid than undo the cursor increment.
            if (index >= data.Count)
            {
                Interlocked.Decrement(ref current);
                return Read();
            }

            // Allocate data to the read calling thread.

            threadAllocatedData[Thread.CurrentThread] = data[index];
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
            get
            {
                return threadAllocatedData[Thread.CurrentThread][name];
            }
        }

        public object this[int i]
        {
            get { return threadAllocatedData[Thread.CurrentThread].Values.ElementAt(i); }
        }
    }
}
