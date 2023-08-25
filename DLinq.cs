using MPI;

public static class DLinq
{
    public static IEnumerable<TResult> DSelect<T, TResult>(this IEnumerable<T> input, Intracommunicator comm, Func<T, TResult> selector)
    {
        var totalRecords = input.Count();
        var batchSize = GetBatchSize(comm.Size, totalRecords);
        var temp = new TResult[] { };
        var result = new List<TResult>();
        for (int rank = 0; rank < comm.Size; rank++)
        {
            if (rank == comm.Rank)
            {
                var skip = rank * batchSize.Item1;
                var take = rank != comm.Size - 1
                    ? batchSize.Item1
                    : batchSize.Item2;

                Console.WriteLine($"(DSelect) Rank {comm.Rank} take {take} records");

                temp = input
                    .Skip(skip)
                    .Take(take)
                    .Select(selector)
                    .ToArray();
            }
        }

        comm.Barrier();
        var gather = comm.Gather(temp, 0);
        if (comm.Rank == 0)
            result = gather.SelectMany(x => x).ToList();

        comm.Broadcast(ref result, 0);
        return result;
    }

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

    private static Tuple<int, int> GetBatchSize(int processes, int totalRecords)
    {
        var batchSize = (decimal)totalRecords / processes;
        var firstBatchSize = Convert.ToInt32(Math.Floor(batchSize));
        var lastBatchSize = Convert.ToInt32(Math.Ceiling(firstBatchSize + ((decimal)totalRecords % processes)));
        return Tuple.Create(firstBatchSize, lastBatchSize);
    }

    public static IEnumerable<TResult> DRun<T, TResult>(this IEnumerable<T> input, Intracommunicator comm, Func<IEnumerable<T>, TResult> func)
    {
        var totalRecords = input.Count();
        var batchSize = GetBatchSize(comm.Size, totalRecords);
        var result = new TResult[] { };
        for (int rank = 0; rank < comm.Size; rank++)
        {
            if (rank == comm.Rank)
            {
                var skip = rank * batchSize.Item1;
                var take = rank != comm.Size - 1
                    ? batchSize.Item1
                    : batchSize.Item2;

                Console.WriteLine($"(DRun) Rank {comm.Rank} take {take} records");
                var temp = func(input.Skip(skip).Take(take));

                comm.Barrier();
                var gather = comm.Gather(temp, 0);
                if (comm.Rank == 0)
                    result = gather;

                // comm.Broadcast(ref result, 0);
            }
        }

        return result;
    }
}