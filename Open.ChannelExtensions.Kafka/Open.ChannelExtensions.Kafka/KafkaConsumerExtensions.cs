namespace Open.ChannelExtensions.Kafka;

public static class KafkaConsumerExtensions
{
	/// <summary>
	/// Creates a Channel Reader from a Kafka Consumer.
	/// </summary>
	/// <param name="consumer">The Kafka Consumer.</param>
	/// <param name="capacity">The bounded capacity of the channel.</param>
	/// <param name="singleReader">Sets whether or not the channel reader can handle multiple simultaneous requests.</param>
	/// <param name="logger">The optional logger to log events to.</param>
	/// <param name="cancellationToken">The optional cancellation token to cancel the consumer.</param>
	/// <remarks>
	/// Set the <paramref name="singleReader"/> parameter to <see langword="true"/> if it's assured that the channel reader will only have one read at a time;<br/>
	/// otherwise leave it as <see langword="false"/> if potentially multiple read threads could be active.
	/// </remarks>
	public static ChannelReader<ConsumeResult<TKey, TValue>> ToChannelReader<TKey, TValue>(
		this IConsumer<TKey, TValue> consumer,
		int capacity, bool singleReader, ILogger? logger, CancellationToken cancellationToken = default)
	{
		var channel = Channel.CreateBounded<ConsumeResult<TKey, TValue>>(new BoundedChannelOptions(capacity)
		{
			SingleReader = singleReader,
			SingleWriter = false
		});

		_ = Task.Run(async () =>
		{
			logger?.LogInformation("Kafka Channel Reader Consumer: Starting");
			var writer = channel.Writer;
			try
			{
				while (!cancellationToken.IsCancellationRequested)
				{
					var result = consumer.Consume(cancellationToken);
					logger?.LogTrace("Kafka Channel Reader Consumer: Consumed Message");
					await writer
						.WriteAsync(result, cancellationToken)
						.ConfigureAwait(false);
				}

				writer.Complete();
				logger?.LogWarning("Kafka Channel Reader Consumer: Cancelled");
			}
			catch (OperationCanceledException ocex)
			{
				writer.Complete();
				logger?.LogError(ocex, "Kafka Channel Reader Consumer: Cancelled");
			}
			catch (Exception ex)
			{
				writer.Complete(ex);
				logger?.LogError("Kafka Channel Reader Consumer: Error");
			}
		}, cancellationToken);

		return channel.Reader;
	}

	/// <inheritdoc cref="ToChannelReader{TKey, TValue}(IConsumer{TKey, TValue}, int, bool, ILogger?, CancellationToken)"/>
	public static ChannelReader<ConsumeResult<TKey, TValue>> ToChannelReader<TKey, TValue>(
		this IConsumer<TKey, TValue> consumer,
		int capacity, ILogger? logger, CancellationToken cancellationToken = default)
		=> consumer.ToChannelReader(capacity, false, logger, cancellationToken);

	/// <inheritdoc cref="ToChannelReader{TKey, TValue}(IConsumer{TKey, TValue}, int, bool, ILogger?, CancellationToken)"/>
	public static ChannelReader<ConsumeResult<TKey, TValue>> ToChannelReader<TKey, TValue>(
		this IConsumer<TKey, TValue> consumer,
		int capacity,
		bool singleReader,
		CancellationToken cancellationToken = default)
		=> consumer.ToChannelReader(capacity, singleReader, null, cancellationToken);

	/// <inheritdoc cref="ToChannelReader{TKey, TValue}(IConsumer{TKey, TValue}, int, bool, ILogger?, CancellationToken)"/>
	public static ChannelReader<ConsumeResult<TKey, TValue>> ToChannelReader<TKey, TValue>(
		this IConsumer<TKey, TValue> consumer,
		int capacity,
		CancellationToken cancellationToken = default)
		=> consumer.ToChannelReader(capacity, false, null, cancellationToken);

	/// <summary>
	/// Creates an channel-buffered Async-Enumerable from a Kafka Consumer.
	/// </summary>
	public static IAsyncEnumerable<ConsumeResult<TKey, TValue>> ToAsyncEnumerable<TKey, TValue>(
		this IConsumer<TKey, TValue> consumer,
		int buffer,
		ILogger? logger,
		CancellationToken cancellationToken = default)
		=> consumer
			.ToChannelReader(buffer, true, logger, cancellationToken)
			.ReadAllAsync(cancellationToken);

	/// <inheritdoc cref="ToAsyncEnumerable{TKey, TValue}(IConsumer{TKey, TValue}, int, ILogger?, CancellationToken)"/>/>
	public static IAsyncEnumerable<ConsumeResult<TKey, TValue>> ToAsyncEnumerable<TKey, TValue>(
		this IConsumer<TKey, TValue> consumer,
		int buffer,
		CancellationToken cancellationToken = default)
		=> consumer
			.ToChannelReader(buffer, true, null, cancellationToken)
			.ReadAllAsync(cancellationToken);
}
