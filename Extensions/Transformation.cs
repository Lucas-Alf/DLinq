using DLinq.Stream;

namespace DLinq.Extensions
{
    public static partial class Extensions
    {
        public static DLinqStream<TResult> Transformation<T, TResult>(this DLinqStream<T> input, Func<List<T>, TResult> func)
        {
            var comm = input.Communicator;
            if (comm.Rank != 0)
            {
                while (true)
                {
                    var msg = comm.Receive<List<T>>(0, 0);

                    var result = func(msg);
                    comm.Send(result, 0, 0);
                }
            }

            return new DLinqStream<TResult>(comm);
        }
    }
}