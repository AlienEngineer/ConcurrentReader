using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConcurrentReader
{

    /// <summary>
    /// Represents a collection of values
    /// </summary>
    public interface ITuple
    {

        /// <summary>
        /// Gets the columns.
        /// </summary>
        /// <value>
        /// The columns.
        /// </value>
        IEnumerable<String> Columns { get; }

        /// <summary>
        /// Gets the value of the given column.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="column">The column.</param>
        /// <returns></returns>
        T GetValue<T>(String column);

        /// <summary>
        /// Gets the value of the given column.
        /// </summary>
        /// <param name="column">The column.</param>
        /// <returns></returns>
        Object GetValue(String column);

    }

    public class Tuple : ITuple
    {
        private readonly IDictionary<String, Object> data;

        public Tuple(IDictionary<String, Object> data)
        {
            this.data = data;
        }

        public IEnumerable<String> Columns { get { return data.Keys; } }

        public T GetValue<T>(String column)
        {
            return (T)Convert.ChangeType(GetValue(column), typeof(T));
        }

        public object GetValue(string column)
        {
            return data[column.ToLower()];
        }
    }
}
