using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

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
            var ts = new HashSet<Task>();
            var reader = GetConcurrentReader();
            var records = 0;
            
            for (int i = 0; i < Environment.ProcessorCount; i++)
            {
                ts.Add(Task.Factory.StartNew(() =>
                {
                    while (reader.Read())
                    {
                        SimulateWork();
                        Interlocked.Increment(ref records);
                    }
                }));
            }

            reader.Close();

            Task.WaitAll(ts.ToArray());

            Thread.MemoryBarrier();
            Assert.AreEqual(RECORD_COUNT, records);
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
    }
}
