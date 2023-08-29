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
                    var msg = comm.Receive<object>(0, 0);
                    if (msg is string && msg.ToString() == DLinqStream<T>.EOS)
                    {
                        comm.Send(DLinqStream<T>.EOS, 0, 0);
                        break;
                    }

                    var data = (List<T>)msg;
                    var result = func(data);
                    comm.Send(result, 0, 0);
                }
            }

            return new DLinqStream<TResult>(comm);
        }
    }
}