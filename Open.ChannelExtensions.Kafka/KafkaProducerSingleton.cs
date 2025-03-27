namespace Open.ChannelExtensions.Kafka;

/// <summary>
/// A singleton wrapper for Kafka producers.
/// Builds and holds on to a single Kafka producer instance.
/// </summary>
/// <param name="builderFactory">
/// A delegate for creating new producer builders.
/// Since the typical pattern for a builder is to only be used once.
/// </param>
/// <param name="logger">
/// The logger to use for logging errors from Kafka..
/// </param>
/// <remarks>
/// If the producer produces errors, it will be disposed and a new one will be created.
/// Disposing of this instance will simply  dispose of the producer.
/// </remarks>
public sealed class KafkaProducerSingleton<TKey, TValue>(
	Func<ProducerBuilder<TKey, TValue>> builderFactory,
	ILogger<KafkaProducerSingleton<TKey, TValue>>? logger)
	: IKafkaProducerProvider<TKey, TValue>, IDisposable
{
	/// <summary>
	/// Creates a new instance of the producer singleton.
	/// </summary>
	public KafkaProducerSingleton(
		ProducerConfig config,
		ILogger<KafkaProducerSingleton<TKey, TValue>>? logger)
		: this(() => new ProducerBuilder<TKey, TValue>(config), logger) { }

	/* It is critical that there only be one producer
	 * instance active (not disposed) at a time.
	 * 
	 * Optimisitc concurrency is not an option here
	 * because if more than one producer is created,
	 * one of them might stay active indefinitely.
	 * 
	 * Or even worse, more continue to be created
	 * and eventually exhaust available connections.
	 * 
	 * We should also track the failure rate
	 * in order to determine a delay before allowing
	 * subsequent producer creation.
	 */

	private Func<ProducerBuilder<TKey, TValue>>? _builderFactory
		= builderFactory ?? throw new ArgumentNullException(nameof(builderFactory));

	private Func<ProducerBuilder<TKey, TValue>> GetBuilderFactory()
		=> _builderFactory ?? throw new ObjectDisposedException(nameof(KafkaProducerSingleton<TKey, TValue>));

	private Lazy<Task<IProducer<TKey, TValue>>>? _producerLazy;

	#region IDisposable Support
	/// <summary>
	/// We need a <see cref="CancellationTokenSource"/> for disposing of the producer task
	/// and preventing a stray producer from being created.
	/// </summary>
	private readonly CancellationTokenSource _cts = new();

	/// <summary>
	/// Finalizer to ensure the producer is disposed.
	/// </summary>
	/// <remarks>
	/// This is a rare case where we should actually
	///	follow the dispose pattern as we are holding
	/// onto a resource that needs to be disposed.
	/// </remarks>
	~KafkaProducerSingleton() => Dispose(false);

	/// <summary>
	/// Disposes of the producer singleton.
	/// </summary>
	/// <remarks>Thread-safe: can be called multiple times but will only be disposed once.</remarks>
	public void Dispose()
	{
		GC.SuppressFinalize(this);
		Dispose(true);
	}

	/// <summary>
	/// Disposes of the producer if it exists.
	/// </summary>
	/// <param name="disposing">
	/// <see langword="true"/> means this is being disposed manually.
	/// <see langword="false"/> means this is being disposed by the GC.
	/// </param>
	private void Dispose(bool disposing)
	{
		// If disposing is false, then the GC has called this method.
		// It means we are safe to dispose of managed resources without concern of re-entrancy.
		// We should not set any fields to null as the GC will do that for us
		// and making changes just creates more total work for the GC.

		if (disposing)
		{
			// The builder factory is the flag to determine if we've been disposed.
			// Check if it's already been set to null.
			var bf = _builderFactory;

			// Is already null? Then someone else already disposed of us.
			if (bf is null) return;
			if (Interlocked.CompareExchange(ref _builderFactory, null, bf) != bf)
				return;
		}
		// Else can only happen if it hasn't been disposed yet and the GC is calling this method.

		using var cts = _cts;
		cts.Cancel();

		DisposeProducer(_producerLazy, !disposing);
	}

	/// <summary>
	/// Ensures the producer is disposed.
	/// </summary>
	/// <param name="producer">The producer to dispose.</param>
	/// <param name="final"><see langword="true"/> signals that this is being done by the finalizer.</param>
	private void DisposeProducer(Lazy<Task<IProducer<TKey, TValue>>>? producer, bool final = false)
	{
		if (producer is null) return;

		// Ensure the instance has been removed from potential use.
		// But no need to set to null if we are being finalized by the GC.
		if (!final) Interlocked.CompareExchange(ref _producerLazy, null, producer);

		if (!producer.IsValueCreated) return;

		// If the value was created, that means it has been used.
		var task = producer.Value;

		// If the task was faulted or cancelled, then it never created a producer.
		if (task.IsFaulted || task.IsCanceled || !task.IsCompleted) return;
		// If it hasn't completed, then we rely on the cancellation token to prevent a stray producer.

		try
		{
			task.Result.Dispose();
		}
		catch (Exception ex)
		{
			logger?.LogDebug(ex, "An error occurred when disposing the producer.");
		}
	}
	#endregion

	#region Failure Handling
	private int _producerFailures;
	private DateTimeOffset _lastFailure = DateTimeOffset.MinValue;

	private void RecordFailure(Error error)
	{
		_lastFailure = DateTimeOffset.Now;
		int count = Interlocked.Increment(ref _producerFailures);
		logger?.LogError("Kafka Producer Error: ({Count}) {Reason}", count, error.Reason);
	}

	private TimeSpan GetProducerDelay()
	{
		int failures = _producerFailures;
		if (failures == 0) return TimeSpan.Zero;

		if (failures != 1 && _lastFailure.AddMinutes(5) < DateTimeOffset.Now)
		{
			// If it's been more than 5 minutes since the last failure, reset the count.
			Interlocked.CompareExchange(ref _producerFailures, 1, failures);
		}

		// Exponential backoff with a max delay.
		int n = Math.Min(_producerFailures, 4) - 1;

		// 5, 10, 20, 40
		return TimeSpan.FromSeconds(5 /*seconds*/ * Math.Pow(2, n));
	}
	#endregion

	/// <summary>
	/// Using a Lazy will properly setup the producer
	/// but will guarantee that when .Value is called,
	/// only one producer is shared.
	/// </summary>
	private Lazy<Task<IProducer<TKey, TValue>>> CreateProducerLazy()
	{
		// Assert early that we are not disposed.
		_ = GetBuilderFactory();

		Lazy<Task<IProducer<TKey, TValue>>>? producer = null;
		return producer = new Lazy<Task<IProducer<TKey, TValue>>>(() =>
		{
			// Keep checking if we are disposed.
			_ = GetBuilderFactory();
			return Task.Run(async () =>
			{
				// Ok, now get the builder.
				var builder
					= GetBuilderFactory()();

				// Configure logging and error handling.
				if (logger is not null)
					builder.SetLogHandler((_, log) => logger.LogDebug("Kafka Log: {Message}", log.Message));

				builder
					.SetErrorHandler((_, error) =>
					{
						// This is a fatal error, we should dispose of the producer and log the failure.
						DisposeProducer(producer);
						RecordFailure(error);
					});

				// If we need to delay, do so now.
				var delay = GetProducerDelay();
				if (delay != TimeSpan.Zero)
					await Task.Delay(delay, _cts.Token).ConfigureAwait(false);

				// Just in case.
				_cts.Token.ThrowIfCancellationRequested();

				// Ok build it.
				return builder.Build();
			}, _cts.Token)
			.ContinueWith(t =>
			{
				// Ensure we dispose of the producer if the task is faulted or canceled.
				if (t.IsFaulted)
				{
					DisposeProducer(producer);
					logger?.LogError(t.Exception, "Building a producer failed.");
				}
				else if (t.IsCanceled)
				{
					DisposeProducer(producer);
					logger?.LogWarning("A producer was being created while the singleton was disposed.");
				}
				else
				{
					// In the rare case we make it here, we should ensure the cancellation token is checked.
					_cts.Token.ThrowIfCancellationRequested();
				}

				return t;
			}, _cts.Token, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Current)
			.Unwrap();
		});
	}

	/// <exception cref="ObjectDisposedException">If this instance is already disposed.</exception>
	/// <inheritdoc />
	[SuppressMessage("Roslynator",
		"RCS1229:Use async/await when necessary",
		Justification = "Returning the task directly is the intent.")]
	public Task<IProducer<TKey, TValue>> GetProducerAsync()
	{
		// Assert early that we are not disposed.
		_ = GetBuilderFactory();

		var producerLazy
			= _producerLazy
			?? LazyInitializer.EnsureInitialized(ref _producerLazy, CreateProducerLazy)!;

		// If accessing the lazy causes an error, we need to handle it here.
		try
		{
			return producerLazy.Value;
		}
		catch (Exception ex)
		{
			// Since the the lazy faulted here then there is no actual producer to dispose.
			Interlocked.CompareExchange(ref _producerLazy, null, producerLazy);
			logger?.LogCritical(ex, "Creating a Kafka producer failed. Possible configuration issue.");

			// This is a very serious error and should not be retried.
			throw;
		}
	}
}
