using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;

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
		int capacity,
		bool singleReader,
		ILogger? logger,
		CancellationToken cancellationToken = default)
	{
		if (consumer is null) throw new ArgumentNullException(nameof(consumer));
		if (capacity < 1) throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be at least 1.");
		Contract.EndContractBlock();

		var channel = Channel.CreateBounded<ConsumeResult<TKey, TValue>>(new BoundedChannelOptions(capacity)
		{
			SingleReader = singleReader,
			SingleWriter = false
		});

		const string LogPrefix = "Kafka Consumer Channel Reader";

		_ = Task.Run(async () =>
		{
			logger?.LogInformation($"{LogPrefix}: starting");
			var writer = channel.Writer;
			try
			{
				while (!cancellationToken.IsCancellationRequested)
				{
					var result = consumer.Consume(cancellationToken);
					logger?.LogTrace($"{LogPrefix}: consumed message");
					await writer
						.WriteAsync(result, cancellationToken)
						.ConfigureAwait(false);
				}

				writer.Complete();
				logger?.LogWarning($"{LogPrefix}:  cancelled");
			}
			catch (OperationCanceledException ocex)
			{
				writer.Complete();
				logger?.LogError(ocex, $"{LogPrefix}: cancelled");
			}
			catch (Exception ex)
			{
				writer.Complete(ex);
				logger?.LogError($"{LogPrefix}: error");
			}
		}, cancellationToken);

		return channel.Reader;
	}

	/// <inheritdoc cref="ToChannelReader{TKey, TValue}(IConsumer{TKey, TValue}, int, bool, ILogger?, CancellationToken)"/>
	public static ChannelReader<ConsumeResult<TKey, TValue>> ToChannelReader<TKey, TValue>(
		this IConsumer<TKey, TValue> consumer,
		int capacity,
		ILogger? logger,
		CancellationToken cancellationToken = default)
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
	/// <remarks>Not LINQ compatibile as</remarks>
	public static IAsyncEnumerable<ConsumeResult<TKey, TValue>> ToBufferedAsyncEnumerable<TKey, TValue>(
		this IConsumer<TKey, TValue> consumer,
		int bufferSize,
		ILogger? logger,
		CancellationToken cancellationToken = default)
	{
		if (bufferSize < 1)
			throw new ArgumentOutOfRangeException(nameof(bufferSize), "Buffer size must be at least 0.");
		Contract.EndContractBlock();

		return bufferSize == 0
			? consumer
				.AsAsyncEnumerable(logger, cancellationToken)
			: consumer
				.ToChannelReader(bufferSize, true, logger, cancellationToken)
				.ReadAllAsync(CancellationToken.None); // We must allow the channel to drain.
	}

	/// <inheritdoc cref="ToAsyncEnumerable{TKey, TValue}(IConsumer{TKey, TValue}, int, ILogger?, CancellationToken)"/>/>
	public static IAsyncEnumerable<ConsumeResult<TKey, TValue>> ToBufferedAsyncEnumerable<TKey, TValue>(
		this IConsumer<TKey, TValue> consumer,
		int bufferSize,
		CancellationToken cancellationToken = default)
		=> consumer.ToBufferedAsyncEnumerable(bufferSize, null, cancellationToken);

	/// <summary>
	/// Creates Async-Enumerable from a Kafka Consumer.
	/// </summary>
	/// <remarks>No buffering allows this to be async LINQ compatible.</remarks>
	public static async IAsyncEnumerable<ConsumeResult<TKey, TValue>> AsAsyncEnumerable<TKey, TValue>(
		this IConsumer<TKey, TValue> consumer,
		ILogger? logger,
		[EnumeratorCancellation] CancellationToken cancellationToken = default)
	{
		while (!cancellationToken.IsCancellationRequested)
		{
			ConsumeResult<TKey, TValue> result;

			try
			{
				// Since the consume method can block, we wrap it in task to allow the state machine to yield.
				result = await Task
					.Run(() => consumer.Consume(cancellationToken), cancellationToken)
					.ConfigureAwait(false);
			}
			catch (OperationCanceledException)
			{
				logger?.LogWarning("Kafka Consumer: cancelled");
				yield break;
			}

			yield return result;
		}
	}

	/// <summary>
	/// Consumes messages from Kafka until the cancellation token is cancelled.
	/// </summary>
	/// <remarks>
	/// Will attempt to restart the consumer after 30 seconds if starting or subcribing it fails.
	/// Will attempt to restart the consumer after 5 seconds if a consumption error occurs.
	/// </remarks>
	public static async IAsyncEnumerable<ConsumeResult<TKey, TValue>> ConsumeUntilCancelled<TKey, TValue>(
		this Func<ConsumerBuilder<TKey, TValue>> builderFactory,
		IEnumerable<string> topics,
		ILogger? logger,
		[EnumeratorCancellation] CancellationToken cancellationToken)
	{
		const string LogPrefix = "Kafka Consumer";
		Debug.Assert(builderFactory is not null);

		logger?.LogInformation($"{LogPrefix}: starting");
		TimeSpan delay = TimeSpan.Zero;

		while (!cancellationToken.IsCancellationRequested)
		{
			if (delay != TimeSpan.Zero)
			{
				try
				{
					await Task
						.Delay(delay, cancellationToken)
						.ConfigureAwait(false);
				}
				catch(OperationCanceledException)
				{
					yield break;
				}
			}

			if (cancellationToken.IsCancellationRequested)
				yield break;

			// Try and start the consumer.
			IConsumer<TKey, TValue> consumer;
			try
			{
				consumer = builderFactory().Build();
			}
			catch (Exception ex)
			{
				logger?.LogError(ex, $"{LogPrefix}: error when starting.");
				delay = TimeSpan.FromSeconds(30);
				continue;
			}

			// Consumer started, ensure disposal.
			using var c = consumer; // If continue is called, the consumer will be disposed.

			// Try and subscribe.
			try
			{
				c.Subscribe(topics);
			}
			catch (Exception ex)
			{
				logger?.LogError(ex, $"{LogPrefix}: error when subscribing.");
				delay = TimeSpan.FromSeconds(30);
				continue;
			}

			// Prepare the enumerator.
			// No awaiting yet, so no cancellation to worry about.
			var e = c
				.AsAsyncEnumerable(logger, cancellationToken)
				.GetAsyncEnumerator(cancellationToken);

			ConsumeResult<TKey, TValue> result;

		tryGetNext:
			try
			{
				bool more = await e
					.MoveNextAsync()
					.ConfigureAwait(false);

				if (!more)
				{
					// Cancelled? Or?
					delay = TimeSpan.FromSeconds(30);
					continue;
				}

				result = e.Current;
			}
			catch (OperationCanceledException)
			{
				logger?.LogWarning($"{LogPrefix}: cancelled");
				yield break;
			}
			catch(Exception ex)
			{
				logger?.LogError(ex, $"{LogPrefix}: error when consuming.");
				delay = TimeSpan.FromSeconds(5);
				continue;
			}

			yield return result;
			goto tryGetNext;
		}
	}

	/// <inheritdoc cref="ConsumeUntilCancelled{TKey, TValue}(Func{ConsumerBuilder{TKey, TValue}}, IEnumerable{string}, int, ILogger?, CancellationToken)"/>
	public static IAsyncEnumerable<ConsumeResult<TKey, TValue>> ConsumeUntilCancelled<TKey, TValue>(
		this Func<ConsumerBuilder<TKey, TValue>> builderFactory,
		string topic,
		ILogger? logger,
		CancellationToken cancellationToken)
		=> builderFactory
			.ConsumeUntilCancelled([topic], logger, cancellationToken);
}
