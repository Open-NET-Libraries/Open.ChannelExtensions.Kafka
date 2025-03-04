using System.Diagnostics;
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
	public static IAsyncEnumerable<ConsumeResult<TKey, TValue>> ToAsyncEnumerable<TKey, TValue>(
		this IConsumer<TKey, TValue> consumer,
		int bufferSize,
		ILogger? logger,
		CancellationToken cancellationToken = default)
		=> consumer
			.ToChannelReader(bufferSize, true, logger, cancellationToken)
			.ReadAllAsync(cancellationToken);

	/// <inheritdoc cref="ToAsyncEnumerable{TKey, TValue}(IConsumer{TKey, TValue}, int, ILogger?, CancellationToken)"/>/>
	public static IAsyncEnumerable<ConsumeResult<TKey, TValue>> ToAsyncEnumerable<TKey, TValue>(
		this IConsumer<TKey, TValue> consumer,
		int bufferSize,
		CancellationToken cancellationToken = default)
		=> consumer
			.ToChannelReader(bufferSize, true, null, cancellationToken)
			.ReadAllAsync(cancellationToken);

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
		int bufferSize,
		ILogger? logger,
		[EnumeratorCancellation] CancellationToken cancellationToken)
	{
		const string LogPrefix = "Kafka Consumer";
		Debug.Assert(builderFactory is not null);

		logger?.LogInformation($"{LogPrefix}: starting");

		while (!cancellationToken.IsCancellationRequested)
		{
			// Try and start the consumer.
			IConsumer<TKey, TValue> consumer;
			try
			{
				consumer = builderFactory().Build();
			}
			catch (Exception ex)
			{
				logger?.LogError(ex, $"{LogPrefix}: error when starting.");
				await Task
					.Delay(TimeSpan.FromSeconds(30), cancellationToken)
					.ConfigureAwait(false);
				continue;
			}

			// Consumer started, ensure disposal.
			using var c = consumer;

			// Try and subscribe.
			try
			{
				c.Subscribe(topics);
			}
			catch (Exception ex)
			{
				logger?.LogError(ex, $"{LogPrefix}: error when subscribing.");
				await Task
					.Delay(TimeSpan.FromSeconds(30), cancellationToken)
					.ConfigureAwait(false);
				continue;
			}

			// Prepare the enumerator.
			var e = c
				.ToAsyncEnumerable(bufferSize, logger, cancellationToken)
				.GetAsyncEnumerator(cancellationToken);

			ConsumeResult<TKey, TValue> result;

		tryGetNext:
			try
			{
				bool ok = await e.MoveNextAsync().ConfigureAwait(false);
				if(!ok)
				{
					// No more? A consumption error must have occured.
					await Task
						.Delay(TimeSpan.FromSeconds(5), cancellationToken)
						.ConfigureAwait(false);

					continue;
				}

				result = e.Current;
			}
			catch(OperationCanceledException)
			{
				logger?.LogWarning($"{LogPrefix}: cancelled");
				yield break;
			}

			yield return result;
			goto tryGetNext;
		}
	}
}
