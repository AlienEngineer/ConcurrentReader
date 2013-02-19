using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace ConcurrentReader.Tests
{
    static class StopwatchExtensions
    {

        public static Stopwatch Run(this Stopwatch watch, Action action)
        {
            watch.Start();
            action();
            watch.Stop();
            return watch;
        }

    }
}
