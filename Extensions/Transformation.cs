using DLinq.Stream;

namespace DLinq.Extensions
{
    public static partial class Extensions
    {
        public static DLinqStream<TResult?> Transformation<T, TResult>(this DLinqStream<T> input, Func<DLinqBatch<List<T>>, TResult> func)
        {
            var comm = input.Communicator;
            if (comm.Rank != input.Source && comm.Rank != input.Sink)
            {
                while (true)
                {
                    var batch = comm.Receive<DLinqBatch<List<T>>>(input.Source, 0);
                    if (!batch.EOS)
                        comm.Send(new DLinqBatch<TResult?>(batch.Id, func(batch)), input.Sink, 0);
                    else
                        comm.Send(new DLinqBatch<TResult?>(batch.Id, default, true), input.Sink, 0);
                }
            }

            return new DLinqStream<TResult?>(comm);
        }
    }
}