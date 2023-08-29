namespace DLinq.Extensions
{
    public static partial class Extensions
    {
        private static Tuple<int, int> GetBatchSize(int processes, int totalRecords)
        {
            var batchSize = (decimal)totalRecords / processes;
            var firstBatchSize = Convert.ToInt32(Math.Floor(batchSize));
            var lastBatchSize = Convert.ToInt32(Math.Ceiling(firstBatchSize + ((decimal)totalRecords % processes)));
            return Tuple.Create(firstBatchSize, lastBatchSize);
        }
    }
}