﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConcurrentReader
{

    /// <summary>
    /// Holds one row of data
    /// </summary>
    class DataRow
    {
        public DataRow(int index)
        {
            Index = index;
        }

        public int Index { get; private set; }

        public String[] ColumnNames { get; set; }

        public Object[] Values { get; set; }

        public ITuple ToTuple(IConcurrentDataReader reader)
        {
            var dic = new Dictionary<String, Object>();

            for (int i = 0; i < ColumnNames.Length; i++)
            {
                dic.Add(ColumnNames[i], Values[i]);
            }

            return new Tuple(dic, reader);
        }

    }

    class BlockingDataReader : ConcurrentDataReaderBase
    {
        private readonly ThreadLocal<ITuple> _ConsumerTuple = new ThreadLocal<ITuple>();
        private IDataReader _Reader;

        private readonly Task _LoadDataRows;
        private readonly Task _MapIntoTuples;

        // First stage buffer.
        private readonly BlockingCollection<DataRow> _LoadedRows = new BlockingCollection<DataRow>();
        // Second stage buffer.
        private readonly BlockingCollection<ITuple> _TransformedRows = new BlockingCollection<ITuple>();
        // Final result
        private readonly ICollection<ITuple> _FinalStage = new List<ITuple>();

        private int currentIndex;

        public BlockingDataReader(IDataReader reader, Predicate<IDataReader> readWhile = null)
        {
            _Reader = reader;

            var f = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);

            _LoadDataRows = f.StartNew(() => LoadingWork(readWhile));
            _MapIntoTuples = f.StartNew(() => MapDataRows());
        }

        private void MapDataRows()
        {
            foreach (var row in _LoadedRows.GetConsumingEnumerable())
            {
                var tuple = row.ToTuple(this);
                _TransformedRows.Add(tuple);
                _FinalStage.Add(tuple);
            }
            _TransformedRows.CompleteAdding();
        }

        private void LoadingWork(Predicate<IDataReader> readWhile = null)
        {
            if (readWhile == null)
            {
                readWhile = r => true;
            }

            var index = 0;
            if (_Reader.Read())
            {
                var columns = _Reader.GetColumnNames();
                
                do
                {
                    if (!readWhile(_Reader))
                    {
                        break;
                    }

                    _LoadedRows.Add(new DataRow(index++)
                    {
                        ColumnNames = columns,
                        Values = _Reader.GetValues()
                    });

                } while (_Reader.Read());

            }

            _LoadedRows.CompleteAdding();
            _Reader.Close();

        }

        public override void Dispose()
        {
            if (_Reader != null)
            {
                _Reader.Dispose();
                _Reader = null;
            }
        }

        public override void Close()
        {
            Task.WaitAll(_LoadDataRows, _MapIntoTuples);
        }

        public override ITuple GetData()
        {
            return _ConsumerTuple.Value;
        }

        public override bool Read()
        {
            _ConsumerTuple.Value = _TransformedRows.GetConsumingEnumerable().FirstOrDefault();

            return _ConsumerTuple.Value != null;
        }

        public override IEnumerable<ITuple> GetTuples()
        {
            return _FinalStage;
        }
    }
}
