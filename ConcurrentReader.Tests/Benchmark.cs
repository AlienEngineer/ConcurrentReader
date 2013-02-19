using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace ConcurrentReader.Tests
{
    static class Benchmark
    {
        public static void Run(Action action, Int32 repeatTimes)
        {
            var stopWatchAll = Stopwatch.StartNew();

            stopWatchAll.Start();

            for (int i = 0; i < repeatTimes; i++)
            {

                // do the action here
                action();

            }

            stopWatchAll.Stop();

            var elapsedTimeAll = stopWatchAll.ElapsedMilliseconds;
            var avgElapsedTime = elapsedTimeAll / (double)repeatTimes;

            Debug.WriteLine(string.Format("Looping: {0} times", repeatTimes));
            Debug.WriteLine(string.Format("Elapsed Time: {0} ms", elapsedTimeAll));
            Debug.WriteLine(string.Format("Average: {0} ms", avgElapsedTime));
        }

    }
}
