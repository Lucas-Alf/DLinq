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
        public T Data { get; private set; }

        /// <summary>
        /// Creation time
        /// </summary>
        public DateTime CreatedAt { get; private set; }

        /// <summary>
        /// Creation time from source
        /// </summary>
        public DateTime SourceCreatedAt { get; private set; }

        public DLinqBatch(long id, T data, DateTime sourceCreatedAt)
        {
            Id = id;
            Data = data;
            CreatedAt = DateTime.Now;
            SourceCreatedAt = sourceCreatedAt;
        }
    }
}