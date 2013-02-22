﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConcurrentReader
{
    public static class ReaderExtensions
    {

        /// <summary>
        /// Makes this reader into a Thread Safe reader.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <param name="readWhile">The read while.</param>
        /// <returns></returns>
        public static IConcurrentDataReader MakeConcurrent(this IDataReader reader, Predicate<IDataReader> readWhile = null)
        {
            return new ConcurrentDBReader(reader, readWhile);
        }

        /// <summary>
        /// Iterates the reader and calls the action for every record. 
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <param name="action">The action.</param>
        /// <param name="maxThreads">The max threads.</param>
        public static void ForEach(this IConcurrentDataReader reader, Action<IDataReader> action, int maxThreads)
        {
            var ts = new HashSet<Task>();

            for (int i = 0; i < maxThreads; i++)
            {
                ts.Add(Task.Factory.StartNew(() =>
                {
                    while (reader.Read())
                    {
                        action(reader);
                    }
                }));
            }

            reader.Close();

            Task.WaitAll(ts.ToArray());
        }


        /// <summary>
        /// Iterates the reader and calls the action for every record. 
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <param name="action">The action.</param>
        public static void ForEach(this IConcurrentDataReader reader, Action<IDataReader> action)
        {
            reader.ForEach(action, Environment.ProcessorCount);
        }
    }
}
