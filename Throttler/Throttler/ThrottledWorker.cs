using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Throttler.Configuration;

namespace Throttler
{
    public abstract class ThrottledWorker<TRequest> : IDisposable
    {
        private readonly ThrottleConfiguration Settings;
        public ThrottledWorker(ThrottleConfiguration configuration)
            : this(new Throttle(configuration))
        {
        }

        public ThrottledWorker(Throttle throttle)
        {
            Settings = throttle.Settings;
            _throttle = throttle;

            int workerThreads;
            ThreadPool.GetMaxThreads(out workerThreads, out int completionPortThreads);

            if (Settings.MaxConcurrentThreads != null)
            {
                workerThreads = Settings.MaxConcurrentThreads.Value;
            }

            if (workerThreads <= 0)
                throw new ArgumentNullException($"There must be at least 1 worker thread available.");

            _cancellationTokenSource = new CancellationTokenSource();
            _queue = new BlockingCollection<TRequest>();
            _workers = Enumerable.Range(0, workerThreads).Select(i => BackgroundWorker(_cancellationTokenSource.Token)).ToArray();

            _throttle = new Throttle(Settings);
        }

        public virtual void Enqueue(TRequest request) => _queue.Add(request);

        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly Throttle _throttle;
        private readonly BlockingCollection<TRequest> _queue;
        private readonly Task[] _workers;
        private bool _disposed;

        private async Task BackgroundWorker(CancellationToken cancellationToken) {
            await Task.Yield(); // release to prevent blocking the thread.

            foreach (TRequest request in _queue.GetConsumingEnumerable(cancellationToken))
            {
                using (await _throttle.AcquireLeaseAsync(cancellationToken)) {
                    await DoWorkAsync(request, cancellationToken);
                }
            }
        }

        /// <summary>
        /// The protected region where the shared memory business logic lives.
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        protected abstract Task DoWorkAsync(TRequest request, CancellationToken cancellationToken);

        public Task WaitAsync(CancellationToken cancellationToken = default) {
            _queue.CompleteAdding();
            Task.WaitAll(_workers, cancellationToken);
            return Task.CompletedTask;
        }

        public void Wait(CancellationToken cancellationToken = default) => WaitAsync(cancellationToken).Wait();

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _cancellationTokenSource.Cancel();
                Wait();

                _disposed = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }

    public class ThrottledWorker : ThrottledWorker<Func<CancellationToken, Task>>
    {
        public ThrottledWorker(ThrottleConfiguration configuration) : base(configuration)
        {
        }

        public ThrottledWorker(Throttle throttle) : base(throttle)
        {
        }

        public void Enqueue(Action<CancellationToken> request) => base.Enqueue(c => Task.Run(() => request(c)));
        public void Enqueue(Action request) => base.Enqueue(c => Task.Run(request));
        public void Enqueue(Func<Task> request) => base.Enqueue(c => request());

        protected override async Task DoWorkAsync(Func<CancellationToken, Task> request, CancellationToken cancellationToken) => await request(cancellationToken);
    }
}
