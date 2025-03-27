namespace Open.ChannelExtensions.Kafka;

/// <summary>
/// DI extensions for Kafka producers.
/// </summary>
public static class KafkaConfigExtensions
{
	/// <summary>
	/// Adds a Kafka producer to the service collection using a <see cref="ProducerConfig"/>.
	/// </summary>
	public static IServiceCollection AddKafkaProducer<TKey, TValue>(
		this IServiceCollection services,
		Func<IServiceProvider, ProducerConfig> configFactory)
	{
		services.TryAddSingleton<IKafkaProducerProvider<TKey, TValue>>(
			sp => new KafkaProducerSingleton<TKey, TValue>(
				configFactory(sp),
				sp.GetService<ILogger<KafkaProducerSingleton<TKey, TValue>>>()));

		return services;
	}

	/// <summary>
	/// Adds a Kafka producer to the service collection using a <see cref="ProducerBuilder{TKey, TValue}"/> factory.
	/// </summary>
	public static IServiceCollection AddKafkaProducer<TKey, TValue>(
		this IServiceCollection services,
		Func<IServiceProvider, Func<ProducerBuilder<TKey, TValue>>> builderFactory)
	{
		services.TryAddSingleton<IKafkaProducerProvider<TKey, TValue>>(
			sp => new KafkaProducerSingleton<TKey, TValue>(
				builderFactory(sp),
				sp.GetService<ILogger<KafkaProducerSingleton<TKey, TValue>>>()));

		return services;
	}
}
