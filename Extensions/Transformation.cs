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
                        comm.Send(
                            value: new DLinqBatch<TResult?>(
                                id: batch.Id,
                                data: func(batch),
                                createdAt: batch.CreatedAt
                            ),
                            dest: input.Sink,
                            tag: 0
                        );
                    else
                        comm.Send(
                            value: new DLinqBatch<TResult?>(
                                id: batch.Id,
                                data: default,
                                createdAt: batch.CreatedAt,
                                eos: true
                            ),
                            dest: input.Sink,
                            tag: 0
                        );
                }
            }

            return new DLinqStream<TResult?>(comm);
        }
    }
}