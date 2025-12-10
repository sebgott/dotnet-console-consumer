using Confluent.Kafka;

Console.WriteLine("Starting Kafka Consumer...");

var bootstrapServers = Environment.GetEnvironmentVariable("BOOTSTRAP") ?? "localhost:9092";
var topic = Environment.GetEnvironmentVariable("TOPIC_NAME") ?? "onprem.utv.doc-examples.data.add.v1";

var clientId = Environment.GetEnvironmentVariable("SASL_CLIENT_ID") ?? "team-2s";
var clientSecret = Environment.GetEnvironmentVariable("SASL_CLIENT_SECRET") ?? "1234";
var tokenEndpoint = Environment.GetEnvironmentVariable("SASL_TOKEN_ENDPOINT_URL") ?? "http://localhost:8080/realms/master/protocol/openid-connect/token";
var audience = Environment.GetEnvironmentVariable("SASL_AUDIENCE") ?? "kafka";

var groupId = Environment.GetEnvironmentVariable("CONSUMER_GROUP") ?? "team-2s-canary";

var config = new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = groupId,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true,

    SecurityProtocol = SecurityProtocol.SaslPlaintext,
    SaslMechanism = SaslMechanism.OAuthBearer,
    SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
    SaslOauthbearerClientId = clientId,
    SaslOauthbearerClientSecret = clientSecret,
    SaslOauthbearerTokenEndpointUrl = tokenEndpoint,
    SaslOauthbearerAssertionClaimAud = audience
};

using var consumer = new ConsumerBuilder<string?, byte[]>(config).Build();

consumer.Subscribe(topic);

Console.WriteLine($"Consumer subscribed to topic: {topic}");

var cancelled = false;
var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    Console.WriteLine("Shutting down consumer...");
    e.Cancel = true;
    cancelled = true;
    cts.Cancel();
};

try
{
    while (!cancelled)
    {
        try
        {
            var result = consumer.Consume(cts.Token);

            if (result?.Message == null)
                continue;

            Console.WriteLine("--- Message received ---");
            Console.WriteLine($"Key:   {result.Message.Key}");
            Console.WriteLine($"Value: {Convert.ToBase64String(result.Message.Value)}");
            Console.WriteLine($"Topic: {result.Topic}");
            Console.WriteLine($"Offset:{result.Offset}");
            Console.WriteLine("-----------------------");
        }
        catch (ConsumeException ex)
        {
            Console.WriteLine($"[CONSUME ERROR] {ex.Error.Reason}");
        }
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Consumer cancelled.");
}
finally
{
    consumer.Close();
}
