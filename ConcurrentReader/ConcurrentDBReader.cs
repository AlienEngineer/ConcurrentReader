using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Threading;

namespace ConcurrentReader
{
    public class ConcurrentDBReader : IDataReader
    {
        private readonly List<ITuple> data = new List<ITuple>();
        private readonly Thread loaderThread;
        private readonly IDataReader _Reader;

        private int current;
        private int running;

        private readonly ConcurrentDictionary<Thread, ITuple> threadAllocatedData = new ConcurrentDictionary<Thread, ITuple>();

        public ConcurrentDBReader(IDataReader reader, Predicate<IDataReader> readWhile = null)
        {
            _Reader = reader;
            loaderThread = new Thread(() => LoadingWork(readWhile));

            FieldCount = _Reader.FieldCount;
            Depth = _Reader.Depth;
        }

        private void LoadingWork(Predicate<IDataReader> readWhile = null)
        {
            if (readWhile == null)
            {
                readWhile = r => true;
            }

            while (_Reader.Read())
            {
                if (!readWhile(_Reader))
                {
                    break;
                }

                var row = new Dictionary<String, Object>();
                for (int i = 0; i < _Reader.FieldCount; i++)
                {
                    row[_Reader.GetName(i).ToLower()] = _Reader[i];
                }

                data.Add(new Tuple(row));

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
        }

        public int Depth { get; private set; }

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
            // At times the allocatedData is set to null this makes sure that it is never null.
            while ((threadAllocatedData[Thread.CurrentThread] = data[index]) == null) { }
            return true;
        }

        public int RecordsAffected
        {
            get { throw new NotImplementedException(); }
        }

        public void Dispose()
        {
            if (_Reader == null) return;

            _Reader.Close();
            _Reader.Dispose();
        }

        public int FieldCount { get; private set; }

        #region GETTERS
        public bool GetBoolean(int i)
        {
            return GetData().GetValue<bool>(i);
        }

        public byte GetByte(int i)
        {
            return GetData().GetValue<byte>(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            return GetData().GetValue<char>(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public ITuple GetData()
        {
            try
            {
                return threadAllocatedData[Thread.CurrentThread];
            }
            catch (KeyNotFoundException ex)
            {
                throw new KeyNotFoundException("No data found for the current thread.", ex);
            }
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            return GetData().GetValue<string>(i);
        }

        public DateTime GetDateTime(int i)
        {
            return GetData().GetValue<DateTime>(i);
        }

        public decimal GetDecimal(int i)
        {
            return GetData().GetValue<decimal>(i);
        }

        public double GetDouble(int i)
        {
            return GetData().GetValue<double>(i);
        }

        public Type GetFieldType(int i)
        {
            return GetData().GetValue(i).GetType();
        }

        public float GetFloat(int i)
        {
            return GetData().GetValue<float>(i);
        }

        public Guid GetGuid(int i)
        {
            return GetData().GetValue<Guid>(i);
        }

        public short GetInt16(int i)
        {
            return GetData().GetValue<Int16>(i);
        }

        public int GetInt32(int i)
        {
            return GetData().GetValue<int>(i);
        }

        public long GetInt64(int i)
        {
            return GetData().GetValue<long>(i);
        }

        public string GetName(int i)
        {
            return GetData().GetName(i);
        }

        public int GetOrdinal(string name)
        {
            return GetData().GetValue<int>(name);
        }

        public string GetString(int i)
        {
            return GetData().GetValue<String>(i);
        }

        public object GetValue(int i)
        {
            return this[i];
        }

        public int GetValues(object[] values)
        {
            var tuple = GetData();
            var objects = tuple.GetValues();
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = objects[i];
            }
            return objects.Length;
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
                try
                {
                    return GetData().GetValue(name);
                }
                catch (KeyNotFoundException ex)
                {
                    throw new KeyNotFoundException("The field " + name + " was not found.", ex);
                }
            }
        }

        public object this[int i]
        {
            get
            {
                return GetData().GetValue(i);
            }
        }
    }
}
