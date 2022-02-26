using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Throttler.Configuration;
using Xunit;
using Xunit.Abstractions;

namespace Throttler.Tests
{
    public class ThrottleTest
    {
        private readonly ITestOutputHelper Log;

        public ThrottleTest(ITestOutputHelper log)
        {
            this.Log = log;
        }

        [Fact]
        public void Throttle_CanPreventConcurrentRequests()
        {
            // no more than 2 concurrent threads.
            ThrottleConfiguration configuration = new ThrottleConfiguration
            {
                MaxConcurrentThreads = 2
            };
            Throttle throttle = new Throttle(configuration);
            Task[] threads = new Task[5];

            ManualResetEvent awaiter = new ManualResetEvent(false);

            CancellationTokenSource outerCancellationSource = new CancellationTokenSource(5 * 1000); // 5 seconds

            long success = 0;
            for (int i = 0; i < threads.Length; i++)
            {
                int instance = i;
                Stopwatch sw = new Stopwatch();
                sw.Start();
                threads[i] = Task.Run(async () => {
                    CancellationTokenSource innerCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(
                        outerCancellationSource.Token,
                        new CancellationTokenSource(5 * 1000).Token  // 5 seconds
                    );
                    Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: thread activated (activateTime: {sw.Elapsed})");
                    sw.Restart();
                    awaiter.WaitOne();
                    Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: thread started (runTime: {sw.Elapsed})");

                    try
                    {
                        IDisposable lease = await throttle.AcquireLeaseAsync(innerCancellationSource.Token);
                        using (lease)
                        {
                            Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: acquired lease (threads: {throttle.ActiveThreads}, runTime: {sw.Elapsed})");

                            await Task.Delay(1000, innerCancellationSource.Token); // do some work

                            Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: completed work (runTime: {sw.Elapsed})");

                            Assert.True(throttle.ActiveThreads <= configuration.MaxConcurrentThreads, $"Instance {instance}: Thread count exceeds max concurrent threads (actual: {throttle.ActiveThreads}, max: {configuration.MaxConcurrentThreads})");
                        }
                        long current = Interlocked.Increment(ref success);
                        Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: thread complete (runTime: {sw.Elapsed}, success: {current})");
                    }
                    catch (Exception ex)
                    {
                        Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: thread failed (runTime: {sw.Elapsed}) {ex}");
                        throw new Exception("Operation failed", ex);
                    }
                });
            }

            Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] set.....");
            awaiter.Set();
            Task.WaitAll(threads, outerCancellationSource.Token);

            Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] complete.....");
            long actual = Interlocked.Read(ref success);
            Assert.Equal(threads.Length, actual);
        }

        [Fact]
        public void Throttle_CanWaitBetweenRequests_FromStartOfLastRequest()
        {
            // no more than 1 thread per second from start of last request.
            ThrottleConfiguration configuration = new ThrottleConfiguration
            {
                WaitTimeBetweenLease = TimeSpan.FromSeconds(1),
                WaitStrategy = ThrottleWaitStrategy.FromStartOfLastRequest
            };
            Throttle throttle = new Throttle(configuration);
            Task[] threads = new Task[5];

            ManualResetEvent awaiter = new ManualResetEvent(false);

            CancellationTokenSource outerCancellationSource = new CancellationTokenSource(12 * 1000); // 12 seconds

            DateTimeOffset? lastRequestTime = null;
            object _lock = new object();

            long success = 0;
            for (int i = 0; i < threads.Length; i++)
            {
                int instance = i;
                Stopwatch sw = new Stopwatch();
                sw.Start();
                threads[i] = Task.Run(() => {
                    awaiter.WaitOne();
                    Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: thread started");

                    using (throttle.AcquireLease())
                    {
                        TimeSpan? timeSinceLastRequest;
                        lock (_lock)
                        {
                            timeSinceLastRequest = DateTimeOffset.Now - lastRequestTime;

                            if (lastRequestTime == null || throttle.LastRequestTime > lastRequestTime)
                                lastRequestTime = throttle.LastRequestTime;
                        }

                        Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: aquired lease (threads: {throttle.ActiveThreads}, timeSinceLastRequest: {timeSinceLastRequest})");

                        Assert.True(throttle.ActiveThreads <= configuration.MaxConcurrentThreads, $"Instance {instance}: Thread count exceeds max concurrent threads (actual: {throttle.ActiveThreads}, max: {configuration.MaxConcurrentThreads})");

                        if (timeSinceLastRequest != null)
                        {
                            Assert.True(timeSinceLastRequest >= configuration.WaitTimeBetweenLease, $"Instance {instance}: Wait time exceeds min wait time (actual: {timeSinceLastRequest}, min: {configuration.WaitTimeBetweenLease})");
                        }

                        Thread.Sleep(1000); // do some work

                        long current = Interlocked.Increment(ref success);
                        Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: completed work");
                    }

                    Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: released lease (threads: {throttle.ActiveThreads})");
                });
            }

            Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] set.....");
            awaiter.Set();
            Task.WaitAll(threads, CancellationToken.None);

            long actual = Interlocked.Read(ref success);
            Assert.Equal(threads.Length, actual);
        }

        [Fact]
        public void Throttle_CanWaitBetweenRequests_FromEndOfLastRequest()
        {
            // no more than 1 thread per second from end of last request
            ThrottleConfiguration configuration = new ThrottleConfiguration
            {
                WaitTimeBetweenLease = TimeSpan.FromSeconds(1),
                WaitStrategy = ThrottleWaitStrategy.FromEndOfLastRequest
            };

            Throttle throttle = new Throttle(configuration);
            Task[] threads = new Task[5];

            ManualResetEvent awaiter = new ManualResetEvent(false);

            for (int i = 0; i < threads.Length; i++)
            {
                int instance = i;
                threads[i] = Task.Run(() => {
                    awaiter.WaitOne();
                    Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: thread started");

                    using (throttle.AcquireLease())
                    {
                        TimeSpan? timeSinceLastRequest = DateTimeOffset.Now - throttle.LastFinishTime;

                        Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: aquired lease (threads: {throttle.ActiveThreads}, timeSinceLastRequest: {timeSinceLastRequest})");

                        Assert.True(throttle.ActiveThreads <= configuration.MaxConcurrentThreads, $"Instance {instance}: Thread count exceeds max concurrent threads (actual: {throttle.ActiveThreads}, max: {configuration.MaxConcurrentThreads})");

                        if (timeSinceLastRequest != null)
                        {
                            Assert.True(timeSinceLastRequest >= configuration.WaitTimeBetweenLease, $"Instance {instance}: Wait time exceeds min wait time (actual: {timeSinceLastRequest}, min: {configuration.WaitTimeBetweenLease})");
                        }

                        Thread.Sleep(1000); // do some work

                        Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: completed work");
                    }

                    Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: released lease (threads: {throttle.ActiveThreads})");
                });
            }

            Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] set.....");
            awaiter.Set();
            Task.WaitAll(threads);
        }

        [Fact]
        public void Throttle_CanWaitBetweenRequests_SlidingWindow()
        {
            // no more than 2 request per 5 seconds
            ThrottleConfiguration configuration = new ThrottleConfiguration
            {
                WaitTimeBetweenLease = TimeSpan.FromSeconds(5),
                WaitStrategy = ThrottleWaitStrategy.SlidingWindow,
                MaxRequests = 2
            };

            Throttle throttle = new Throttle(configuration);
            Task[] threads = new Task[5];

            ManualResetEvent awaiter = new ManualResetEvent(false);

            for (int i = 0; i < threads.Length; i++)
            {
                int instance = i;
                threads[i] = Task.Run(() => {
                    awaiter.WaitOne();
                    Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: thread started");

                    using (throttle.AcquireLease())
                    {
                        TimeSpan? timeSinceLastRequest = DateTimeOffset.Now - throttle.LastFinishTime;

                        Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: aquired lease (window: {throttle.RateWindow?.DateTime:yyyy-MM-dd HH:mm:ss.fff}, threads: {throttle.ActiveThreads}, requests: {throttle.RequestCount})");

                        //Assert.True(throttle.ActiveThreads <= configuration.MaxConcurrentThreads, $"Instance {instance}: Thread count exceeds max concurrent threads (actual: {throttle.ActiveThreads}, max: {configuration.MaxConcurrentThreads})");

                        Assert.True(throttle.RequestCount <= configuration.MaxRequests, $"Instance {instance}: Request count exceeds max request count (actual: {throttle.RequestCount}, max: {configuration.MaxRequests})");

                        Thread.Sleep(1000); // do some work

                        Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: completed work");
                    }

                    Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: released lease (threads: {throttle.ActiveThreads})");
                });
            }

            Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] set.....");
            awaiter.Set();
            Task.WaitAll(threads);
        }

        [Fact]
        public void Throttle_CanWaitBetweenRequests_TumblingWindow()
        {
            // No more than 2 requests per 5 seconds and only 1 concurrent thread.
            ThrottleConfiguration configuration = new ThrottleConfiguration
            {
                WaitTimeBetweenLease = TimeSpan.FromSeconds(5),
                WaitStrategy = ThrottleWaitStrategy.TumblingWindow,
                MaxConcurrentThreads = 1,
                MaxRequests = 2
            };

            Throttle throttle = new Throttle(configuration);
            Task[] threads = new Task[5];

            ManualResetEvent awaiter = new ManualResetEvent(false);

            for (int i = 0; i < threads.Length; i++)
            {
                int instance = i;
                threads[i] = Task.Run(() => {
                    awaiter.WaitOne();
                    Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: thread started");

                    using (throttle.AcquireLease())
                    {
                        TimeSpan? timeSinceLastRequest = DateTimeOffset.Now - throttle.LastFinishTime;

                        Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: acquired lease (window: {throttle.RateWindow?.DateTime:yyyy-MM-dd HH:mm:ss.fff}, threads: {throttle.ActiveThreads}, requests: {throttle.RequestCount})");

                        Assert.True(throttle.ActiveThreads <= configuration.MaxConcurrentThreads, $"Instance {instance}: Thread count exceeds max concurrent threads (actual: {throttle.ActiveThreads}, max: {configuration.MaxConcurrentThreads})");

                        Assert.True(throttle.RequestCount <= configuration.MaxRequests, $"Instance {instance}: Request count exceeds max request count (actual: {throttle.RequestCount}, max: {configuration.MaxRequests})");

                        Thread.Sleep(1000); // do some work

                        Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: completed work");
                    }

                    Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] Instance {instance}: released lease (threads: {throttle.ActiveThreads})");
                });
            }

            Log.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] set.....");
            awaiter.Set();
            Task.WaitAll(threads);
        }
    }
}
