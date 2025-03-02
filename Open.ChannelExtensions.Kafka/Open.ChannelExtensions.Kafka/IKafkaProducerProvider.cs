namespace Open.ChannelExtensions.Kafka;

/// <summary>
/// A provider for Kafka producers.
/// </summary>
interface IKafkaProducerProvider<TKey, TValue>
{
	/// <summary>
	/// Gets a producer.
	/// </summary>
	/// <remarks>
	/// A delay may be introduced if the previous producer failed
	/// and a back-off time is needed.
	/// </remarks>
	Task<IProducer<TKey, TValue>> GetProducerAsync();
}
