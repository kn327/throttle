using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Throttler.Configuration;

namespace Throttler
{
    public class Throttle
    {
        public ThrottleConfiguration Settings { get; private set; }

        private List<long> _requests;
        public long? RequestCount
        {
            get
            {
                if (Settings.MaxRequests == null) return null;

                lock (SyncLock)
                {
                    return SyncCountAndWindowUnsafe(out DateTimeOffset periodWindow);
                }
            }
        }

        public DateTimeOffset? RateWindow
        {
            get
            {
                if (Settings.MaxRequests == null) return null;

                lock (SyncLock)
                {
                    SyncCountAndWindowUnsafe(out DateTimeOffset periodWindow);
                    return periodWindow;
                }
            }
        }

        /// <summary>
        /// Determines the window period for this request, syncs the _request history, and returns the count of requests
        /// </summary>
        /// <param name="periodWindow"></param>
        /// <returns></returns>
        private int SyncCountAndWindowUnsafe(out DateTimeOffset periodWindow)
        {
            long now = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            long? waitMs = (long?)Settings.WaitTimeBetweenLease?.TotalMilliseconds;
            switch (Settings.WaitStrategy)
            {
                case ThrottleWaitStrategy.TumblingWindow:
                    if (waitMs != null && waitMs > 0)
                        now -= (now % waitMs.Value);
                    break;
                default:
                    if (waitMs != null && waitMs > 0)
                        now -= waitMs.Value;
                    break;
            }

            int count = 0;

            while (count < _requests.Count)
            {
                if (_requests[count] >= now)
                    break;

                count++;
            }

            if (count > 0) _requests.RemoveRange(0, count);

            switch (Settings.WaitStrategy)
            {
                case ThrottleWaitStrategy.TumblingWindow:
                    periodWindow = DateTimeOffset.FromUnixTimeMilliseconds(now);
                    break;
                case ThrottleWaitStrategy.SlidingWindow:
                    if (_requests.Count > 0)
                        periodWindow = DateTimeOffset.FromUnixTimeMilliseconds(_requests[0]);
                    else
                        periodWindow = DateTimeOffset.FromUnixTimeMilliseconds(now + (waitMs ?? 0));
                    break;
                default: throw new NotSupportedException($"{Settings.WaitStrategy} is not supported.");
            }

            return _requests.Count;
        }

        private long _lastStartTime = 0;
        public DateTimeOffset? LastRequestTime
        {
            get
            {
                lock (SyncLock)
                {
                    if (_lastStartTime == 0) return null;

                    return DateTimeOffset.FromUnixTimeMilliseconds(_lastStartTime);
                }
            }
        }

        private long _lastFinishTime = 0;
        public DateTimeOffset? LastFinishTime
        {
            get
            {
                lock (SyncLock)
                {
                    if (_lastFinishTime == 0) return null;

                    return DateTimeOffset.FromUnixTimeMilliseconds(_lastFinishTime);
                }
            }
        }

        private int _activeThreads = 0;
        public int ActiveThreads
        {
            get
            {
                lock (SyncLock)
                {
                    return _activeThreads;
                }
            }
        }

        private SemaphoreSlim _semaphore;

        public Throttle(ThrottleConfiguration configuration)
        {
            Settings = configuration;

            if (configuration.MaxRequests != null)
            {
                if (configuration.WaitStrategy == null) throw new ArgumentNullException($"{nameof(configuration.WaitStrategy)} is required when {nameof(configuration.MaxRequests)} is specified.");
                if (configuration.WaitTimeBetweenLease == null) throw new ArgumentNullException($"{nameof(configuration.WaitTimeBetweenLease)} is required when {nameof(configuration.MaxRequests)} is specified.");
                if (configuration.MaxRequests <= 0) throw new ArgumentOutOfRangeException($"{nameof(configuration.MaxRequests)} must be greater than 0.");

                if (configuration.MaxConcurrentThreads == null) configuration.MaxConcurrentThreads = configuration.MaxRequests;
                if (configuration.MaxConcurrentThreads > configuration.MaxRequests) throw new ArgumentOutOfRangeException($"{nameof(configuration.MaxConcurrentThreads)} must be less than or equal to {nameof(configuration.MaxRequests)}.");
            }

            if (configuration.WaitTimeBetweenLease != null || configuration.WaitStrategy != null)
            {
                if (configuration.WaitStrategy == null) throw new ArgumentNullException($"{nameof(configuration.WaitStrategy)} is required when {nameof(configuration.WaitTimeBetweenLease)} is specified.");
                if (configuration.WaitTimeBetweenLease == null) throw new ArgumentNullException($"{nameof(configuration.WaitTimeBetweenLease)} is required when {nameof(configuration.WaitStrategy)} is specified.");

                if (configuration.WaitTimeBetweenLease <= TimeSpan.Zero) throw new ArgumentOutOfRangeException($"{nameof(configuration.WaitTimeBetweenLease)} must be greater than 0.");

                switch (configuration.WaitStrategy)
                {
                    case ThrottleWaitStrategy.FromEndOfLastRequest:
                    case ThrottleWaitStrategy.FromStartOfLastRequest:
                        configuration.MaxRequests = 1;
                        configuration.MaxConcurrentThreads = 1;
                        break;
                    case ThrottleWaitStrategy.SlidingWindow:
                    case ThrottleWaitStrategy.TumblingWindow:
                        if (configuration.MaxRequests == null) throw new ArgumentNullException($"{nameof(configuration.MaxRequests)} is required when {nameof(configuration.WaitStrategy)} is {configuration.WaitStrategy}.");
                        _requests = new List<long>();
                        break;
                }
            }

            if (configuration.MaxConcurrentThreads != null)
            {
                if (configuration.MaxConcurrentThreads <= 0) throw new ArgumentOutOfRangeException($"{nameof(configuration.MaxConcurrentThreads)} must be greater than 0.");
                _semaphore = new SemaphoreSlim(configuration.MaxConcurrentThreads.Value, configuration.MaxConcurrentThreads.Value);
            }
        }

        private readonly object SyncLock = new object();
        private async Task WaitForLeaseAsync(CancellationToken cancellationToken)
        {
            await _semaphore?.WaitAsync(cancellationToken);

            do
            {
                TimeSpan? waitTime = null;

                lock (SyncLock)
                {
                    DateTimeOffset now = DateTimeOffset.Now;
                    if (Settings.WaitTimeBetweenLease != null)
                    {
                        DateTimeOffset? nextAllowedTime = null;
                        switch (Settings.WaitStrategy)
                        {
                            case ThrottleWaitStrategy.FromStartOfLastRequest:
                                if (_lastStartTime > 0)
                                    nextAllowedTime = DateTimeOffset.FromUnixTimeMilliseconds(_lastStartTime);
                                break;
                            case ThrottleWaitStrategy.FromEndOfLastRequest:
                                if (_lastFinishTime > 0)
                                    nextAllowedTime = DateTimeOffset.FromUnixTimeMilliseconds(_lastFinishTime);
                                break;
                            case ThrottleWaitStrategy.SlidingWindow:
                            case ThrottleWaitStrategy.TumblingWindow:
                                int requestCount = SyncCountAndWindowUnsafe(out DateTimeOffset periodWindow);

                                if (requestCount >= Settings.MaxRequests)
                                    nextAllowedTime = periodWindow;
                                break;
                            default: throw new NotSupportedException($"{Settings.WaitStrategy} is not supported.");
                        }

                        if (nextAllowedTime != null)
                        {
                            nextAllowedTime = nextAllowedTime?.Add(Settings.WaitTimeBetweenLease.Value);

                            if (nextAllowedTime > now)
                            {
                                waitTime = nextAllowedTime.Value - now;
                            }
                        }
                    }

                    if (waitTime == null)
                    {
                        _activeThreads++;
                        _lastStartTime = now.ToUnixTimeMilliseconds();
                        if (_requests != null)
                            _requests.Add(_lastStartTime);

                        break;
                    }
                }

                try { await Task.Delay(waitTime.Value, cancellationToken); }
                catch { cancellationToken.ThrowIfCancellationRequested(); }
            }
            while (!cancellationToken.IsCancellationRequested);
        }

        private void ReleaseLease()
        {
            lock (SyncLock)
            {
                //_current.Value = null;
                _activeThreads--;
                _lastFinishTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            }
            _semaphore?.Release();
        }

        //private AsyncLocal<Lease> _current { get; } = new AsyncLocal<Lease>();
        public async Task<IDisposable> AcquireLeaseAsync(CancellationToken cancellationToken = default) {
            /*
            lock (SyncLock)
            {
                if (_current.Value != null)
                {
                    switch (Settings.RecursionStrategy)
                    {
                        case ThrottleRecursionStrategy.AllowsRecursion: return Lease.Empty;
                        case ThrottleRecursionStrategy.NoRecursion: throw new LockRecursionException("AcquireLease has already been called in this context. Be sure to properly call dispose prior to calling AcquireLease again.");
                    }
                }
            }
            */

            await WaitForLeaseAsync(cancellationToken);

            //lock (SyncLock)
                return /*_current.Value =*/ new Lease(this);
        }

        public IDisposable AcquireLease(CancellationToken cancellationToken = default) => AcquireLeaseAsync(cancellationToken).Result;

        private class Lease : IDisposable
        {
            public static readonly Lease Empty = new Lease(null);

            private Throttle _throttle;

            public Lease(Throttle throttle)
            {
                _throttle = throttle;
           }

            public void Dispose()
            {
                if (_throttle != null)
                {
                    _throttle.ReleaseLease();
                    _throttle = null;
                }
            }
        }
    }
}
