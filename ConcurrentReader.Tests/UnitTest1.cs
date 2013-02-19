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

        public IDataReader GetReader()
        {
            return connection.ExecuteReader("Select * From Employees");
        }

        public IDataReader GetConcurrentReader()
        {
            return new ConcurrentDBReader(GetReader());
        }

        public double SimulateWork()
        {
            // Simulate work
            double result = 0.0;
            for (int i = 0; i < 250000; i++)
            {
                result += Math.Sqrt(i);
            }
            return result;
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

            Assert.AreEqual(9, count);
        }
        
        [Test]
        public void Timed_Simple_Loading_Test()
        {
            Console.WriteLine("Timed_Simple_Loading_Test - Elapsed Time : {0}", new Stopwatch().Run(Simple_Loading_Test).ElapsedTicks);
        }

        [Test]
        public void Timed_Rush_Simple_Loading_Test()
        {
            long allTicks = 0;
            for (int i = 0; i < 100; i++)
            {
                allTicks += new Stopwatch().Run(Simple_Loading_Test).ElapsedTicks;
            }

            Console.WriteLine("Timed_Rush_Simple_Loading_Test - Elapsed Time Mean : {0}", allTicks / 100.0);
        }

        [Test]
        public void Concurrent_Loading_Test()
        {
            var ts = new HashSet<Task>();
            var reader = GetConcurrentReader();

            while (reader.Read())
            {
                // Simulate async work
                ts.Add(Task.Factory.StartNew(() => SimulateWork()));
            }

            Task.WaitAll(ts.ToArray());

            Assert.AreEqual(9, ts.Count);
        }

        [Test]
        public void Timed_Concurrent_Loading_Test()
        {
            Console.WriteLine("Timed_Concurrent_Loading_Test - Elapsed Time : {0}", new Stopwatch().Run(Concurrent_Loading_Test).ElapsedTicks);
        }

        [Test]
        public void Timed_Rush_Concurrent_Loading_Test()
        {
            long allTicks = 0;
            for (int i = 0; i < 100; i++)
            {
                allTicks += new Stopwatch().Run(Concurrent_Loading_Test).ElapsedTicks;
            }

            Console.WriteLine("Timed_Rush_Concurrent_Loading_Test - Elapsed Time Mean : {0}", allTicks / 100.0);
        }
    }
}
