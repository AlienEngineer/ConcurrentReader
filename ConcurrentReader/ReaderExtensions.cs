using System;
using System.Collections.Concurrent;
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
        /// <returns></returns>
        public static IEnumerable<ITuple> ForEach(this IConcurrentDataReader reader, Action<IConcurrentDataReader> action, int maxThreads)
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

            return reader.GetTuples();
        }

        /// <summary>
        /// Iterates the reader and calls the action for every record. 
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <param name="action">The action.</param>
        public static IEnumerable<ITuple> ForEach(this IConcurrentDataReader reader, Action<IConcurrentDataReader> action)
        {
            return reader.ForEach(action, Environment.ProcessorCount);
        }

        /// <summary>
        /// Iterates the reader and transforms the ITuple instance into TModel type.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="reader">The reader.</param>
        /// <param name="transform">The transform.</param>
        /// <param name="maxThreads">The max threads.</param>
        /// <returns><code>IEnumerable<TModel></code> With the same order as the records were read.</returns>
        public static IEnumerable<TModel> ForEach<TModel>(this IConcurrentDataReader reader, Func<ITuple, TModel> transform,int maxThreads) where TModel : class, new()
        {
            ConcurrentDictionary<ITuple, TModel> models = new ConcurrentDictionary<ITuple, TModel>();
            reader.ForEach(r =>
            {
                var data = r.GetData();
                models[data] = transform(data);
            }, maxThreads);

            return reader.GetTuples().Select(t => models[t]);
        }

        /// <summary>
        /// Iterates the reader and transforms the ITuple instance into TModel type.
        /// </summary>
        /// <typeparam name="TModel">The type of the model.</typeparam>
        /// <param name="reader">The reader.</param>
        /// <param name="transform">The transform.</param>
        /// <returns><code>IEnumerable<TModel></code> With the same order as the records were read.</returns>
        public static IEnumerable<TModel> ForEach<TModel>(this IConcurrentDataReader reader, Func<ITuple, TModel> transform) where TModel : class, new()
        {
            return reader.ForEach(transform, Environment.ProcessorCount);
        }

        /// <summary>
        /// Cache the readers data.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        public static IConcurrentDataReader Load(this IConcurrentDataReader reader)
        {
            reader.ForEach(r => {});
            return reader;
        }
    }
}
