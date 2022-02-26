namespace Throttler.Configuration
{
    public enum ThrottleWaitStrategy
    {
        /// <summary>
        /// From the start datetime of the last request, implicitly single threaded
        /// </summary>
        FromStartOfLastRequest,
        /// <summary>
        /// From the end datetime of the last request, implicitly single threaded
        /// </summary>
        FromEndOfLastRequest,
        /// <summary>
        /// A sliding window is when no more than x number of requests can occur within a period of time and is from start of last request
        /// </summary>
        SlidingWindow,
        /// <summary>
        /// A tumbling window is when no more than x number of requests can occur within a fixed period of time and is from start of time window
        /// </summary>
        TumblingWindow,
    }
}
