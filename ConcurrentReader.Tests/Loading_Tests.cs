using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Collections.Concurrent;
using System.Linq;

namespace ConcurrentReader.Tests
{
    [TestFixture]
    public class Loading_Tests
    {
        readonly Connection connection = new Connection();
        const int RECORD_COUNT = 830;
        const int RUSH_COUNT = 50;

        public IDataReader GetReader()
        {
            return connection.ExecuteReader("Select * From Orders order by OrderId");
        }

        public IDataReader GetConcurrentReader()
        {
            return GetReader().MakeConcurrent();
        }

        public double SimulateWork()
        {
            // Simulate work
            double result = 0.0;
            for (int i = 0; i < 2500; i++)
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
        public void Concurrent_Loading_Test_With_Predicate()
        {
            var reader = GetReader().MakeConcurrent(r => r.GetInt32(0) < 10500);
            int[] records = {0};

            reader.ForEach(r =>
            {
                SimulateWork();
                Interlocked.Increment(ref records[0]);
            });

            Assert.AreEqual(252, Thread.VolatileRead(ref records[0]));
        }

        [Test]
        public void Concurrent_Loading_Test()
        {
            var reader = GetReader().MakeConcurrent();
            int[] records = {0};

            reader.ForEach(r =>
            {
                SimulateWork();
                Interlocked.Increment(ref records[0]);
            });

            Assert.AreEqual(RECORD_COUNT, Thread.VolatileRead(ref records[0]));
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
            
            var values = new ConcurrentStack<IDictionary<String, Object>>();

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

        [Test]
        public void Ordered_Loaded_Data()
        {

            var orders = GetReader().MakeConcurrent().ForEach<Order>(t =>
                        {
                            return new Order
                            {
                                OrderId = t.GetValue<int>("orderId")
                            };
                        });

            Assert.AreEqual(RECORD_COUNT, orders.Count());


            int last = 0;

            foreach (var order in orders)
            {
                Assert.Less(last, order.OrderId);
                last = order.OrderId;
            }

        }

        [Test]
        public void Ordered_Loaded_Data_AsTuples()
        {
            var tuples = GetReader().MakeConcurrent().Load().GetTuples();
            Assert.AreEqual(RECORD_COUNT, tuples.Count());

            int last = 0;

            foreach (var tuple in tuples)
            {
                Assert.Less(last, tuple.GetValue<int>("orderId"));
                last = tuple.GetValue<int>("orderId");
            }
        }

        #endregion

    }
}
