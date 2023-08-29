using DLinq.Extensions;

namespace DLinq.Exemples
{
    public class PrimeNumbers
    {
        public static void Run(MPI.Intracommunicator comm)
        {
            var input = Enumerable.Range(1, 10000000);
            var result = input
                .DRun(comm, (x) =>
                {
                    var total = x.Where(x => IsPrime(x)).Count();
                    Console.WriteLine($"Rank {comm.Rank} found {total} prime numbers!");
                    return total;
                });

            if (comm.Rank == 0)
                Console.WriteLine($"Total prime numbers found: {result.Sum()}");
        }

        private static bool IsPrime(int number)
        {
            if (number <= 1) return false;
            if (number == 2) return true;
            if (number % 2 == 0) return false;

            var boundary = (int)Math.Floor(Math.Sqrt(number));

            for (int i = 3; i <= boundary; i += 2)
                if (number % i == 0)
                    return false;

            return true;
        }
    }
}