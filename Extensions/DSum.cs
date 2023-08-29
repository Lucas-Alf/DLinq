using MPI;

namespace DLinq.Extensions
{
    public static partial class Extensions
    {
        public static int DSum(this IEnumerable<int> input, Intracommunicator comm, Func<int, int> selector = null)
        {
            int result = 0;
            var totalRecords = input.Count();
            var batchSize = GetBatchSize(comm.Size, totalRecords);
            var temp = 0;
            for (int rank = 0; rank < comm.Size; rank++)
            {
                if (rank == comm.Rank)
                {
                    var skip = rank * batchSize.Item1;
                    var take = rank != comm.Size - 1
                        ? batchSize.Item1
                        : batchSize.Item2;

                    Console.WriteLine($"(DSum) Rank {comm.Rank} take {take} records");

                    if (selector != null)
                        temp = input
                            .Skip(skip)
                            .Take(take)
                            .Sum(selector);
                    else
                        temp = input
                            .Skip(skip)
                            .Take(take)
                            .Sum();
                }
            }

            comm.Barrier();
            result = comm.Reduce(temp, Operation<int>.Add, 0);
            comm.Broadcast(ref result, 0);

            return result;
        }
    }
}