using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Threading;

namespace ConcurrentReader
{
    public class ConcurrentDataReader : ConcurrentDataReaderBase
    {
        private readonly List<ITuple> data = new List<ITuple>();
        private readonly Thread loaderThread;
        private readonly IDataReader _Reader;

        private int current;
        private int running;
        
        private readonly ConcurrentDictionary<Thread, ITuple> threadAllocatedData = new ConcurrentDictionary<Thread, ITuple>();

        public ConcurrentDataReader(IDataReader reader, Predicate<IDataReader> readWhile = null)
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

        public override ITuple GetData()
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

        public override void Close()
        {
            while (loaderThread.ThreadState == ThreadState.Unstarted)
            {
                Thread.Sleep(0);
            }

            loaderThread.Join();
        }

        public override void Dispose()
        {
            if (_Reader == null) return;

            _Reader.Close();
            _Reader.Dispose();
        }

        public override bool Read()
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

        public override IEnumerable<ITuple> GetTuples()
        {
            return data;
        }
    }
}
