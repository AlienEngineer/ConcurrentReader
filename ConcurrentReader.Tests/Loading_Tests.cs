using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Concurrent;

namespace ConcurrentReader.Tests
{
    [TestFixture]
    public class Loading_Tests
    {
        Connection connection = new Connection();
        const int RECORD_COUNT = 830;
        const int RUSH_COUNT = 100;

        public IDataReader GetReader()
        {
            return connection.ExecuteReader("Select * From Orders");
        }

        public IDataReader GetConcurrentReader()
        {
            return new ConcurrentDBReader(GetReader());
        }

        public double SimulateWork()
        {
            // Simulate work
            double result = 0.0;
            for (int i = 0; i < 25000; i++)
            {
                result += Math.Sqrt(i);
            }
            return result;
        }
        
        #region Simple Testing Area

        [Test]
        public void Sync_Simple_Loading_Test()
        {
            var count = 0;
            var reader = GetReader();

            while (reader.Read())
            {
                SimulateWork();
                ++count;
            }

            Assert.AreEqual(RECORD_COUNT, count);
        }

        [Test]
        public void Timed_Sync_Simple_Loading_Test()
        {
            Benchmark.Run(Sync_Simple_Loading_Test, 1);
        }

        [Test]
        public void Timed_Rush_Sync_Simple_Loading_Test()
        {
            Benchmark.Run(Sync_Simple_Loading_Test, RUSH_COUNT);
        }

        [Test]
        public void Simple_Loading_Test()
        {
            var count = 0;
            var reader = GetConcurrentReader();

            while (reader.Read())
            {
                SimulateWork();
                ++count;
            }

            Assert.AreEqual(RECORD_COUNT, count);
        }

        [Test]
        public void Timed_Simple_Loading_Test()
        {
            Benchmark.Run(Simple_Loading_Test, 1);
        }

        [Test]
        public void Timed_Rush_Simple_Loading_Test()
        {
            Benchmark.Run(Simple_Loading_Test, RUSH_COUNT);
        }

        [Test]
        public void Concurrent_Loading_Test()
        {
            var reader = GetReader().MakeConcurrent();
            var records = 0;

            reader.ForEach(r =>
            {
                SimulateWork();
                Interlocked.Increment(ref records);
            });

            Assert.AreEqual(RECORD_COUNT, Thread.VolatileRead(ref records));
        }

        [Test]
        public void Timed_Concurrent_Loading_Test()
        {
            Benchmark.Run(Concurrent_Loading_Test, 1);
        }

        [Test]
        public void Timed_Rush_Concurrent_Loading_Test()
        {
            Benchmark.Run(Concurrent_Loading_Test, RUSH_COUNT);
        }
        
        #endregion

        #region

        [Test]
        public void Process_Loaded_Data()
        {
            var reader = GetReader().MakeConcurrent();
            
            ConcurrentStack<IDictionary<String, Object>> values = new ConcurrentStack<IDictionary<String, Object>>();

            var columns = new[] { "OrderId", "CustomerId", "EmployeeID" };

            reader.ForEach(r =>
            {
                var row = new Dictionary<String, Object>();

                foreach (var column in columns)
                {
                    row[column] = r[column];
                }

                values.Push(row);                
            });

            Assert.AreEqual(RECORD_COUNT, values.Count);

        }

        [Test]
        public void Timed_Process_Loaded_Data()
        {
            Benchmark.Run(Process_Loaded_Data, RUSH_COUNT);
        }

        #endregion

    }
}
