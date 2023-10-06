namespace DLinq.Stream
{
    [Serializable]
    public class DLinqBatch<T>
    {
        /// <summary>
        /// Id
        /// </summary>
        public long Id { get; set; }

        /// <summary>
        /// Data
        /// </summary>
        public T? Data { get; private set; }

        /// <summary>
        /// Creation time in seconds.
        /// Equivalent to MPI_Wtime()
        /// </summary>
        public double CreatedAt { get; private set; }

        /// <summary>
        /// End of Stream
        /// </summary>
        public bool EOS { get; private set; }

        public DLinqBatch(long id, T? data, double createdAt, bool eos = false)
        {
            Id = id;
            Data = data;
            CreatedAt = createdAt;
            EOS = eos;
        }
    }
}