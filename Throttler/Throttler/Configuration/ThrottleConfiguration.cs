using System;

namespace Throttler.Configuration
{
    public class ThrottleConfiguration
    {
        public int? MaxConcurrentThreads { get; set; }
        public TimeSpan? WaitTimeBetweenLease { get; set; }
        public ThrottleWaitStrategy? WaitStrategy { get; set; }
        public int? MaxRequests { get; set; }
        //public ThrottleRecursionStrategy RecursionStrategy { get; set; } = ThrottleRecursionStrategy.Default;
    }
}
