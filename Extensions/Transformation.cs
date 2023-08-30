using DLinq.Stream;

namespace DLinq.Extensions
{
    public static partial class Extensions
    {
        public static DLinqStream<TResult> Transformation<T, TResult>(this DLinqStream<T> input, Func<List<T>, TResult> func)
        {
            var comm = input.Communicator;
            if (comm.Rank != input.Source && comm.Rank != input.Sink)
            {
                while (true)
                {
                    var record = comm.Receive<DLinqRecord<List<T>>>(input.Source, 0);
                    if (record.EOS)
                    {
                        // Send EOS to sink
                        comm.Send(new DLinqRecord<TResult>(default!, true), input.Sink, 0);
                        break;
                    }

                    comm.Send(new DLinqRecord<TResult>(func(record.Data)), input.Sink, 0);
                }
            }

            return new DLinqStream<TResult>(comm);
        }
    }
}