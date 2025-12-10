using System.Text.Json;
using Confluent.Kafka;

Console.WriteLine("Starting Kafka Consumer...");

var bootstrapServers = Environment.GetEnvironmentVariable("BOOTSTRAP") ?? "localhost:9092";
var topic = Environment.GetEnvironmentVariable("TOPIC_NAME") ?? "onprem.utv.doc-examples.data.add.v1";

var clientId = Environment.GetEnvironmentVariable("SASL_CLIENT_ID") ?? "team-2s";
var clientSecret = Environment.GetEnvironmentVariable("SASL_CLIENT_SECRET") ?? "1234";
var tokenEndpoint = Environment.GetEnvironmentVariable("SASL_TOKEN_ENDPOINT_URL") ?? "http://localhost:8080/realms/master/protocol/openid-connect/token";

var groupId = Environment.GetEnvironmentVariable("CONSUMER_GROUP") ?? "team-2s-canary";

var config = new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = groupId,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true,

    SecurityProtocol = SecurityProtocol.SaslPlaintext,
    SaslMechanism = SaslMechanism.OAuthBearer
};

using var consumer = new ConsumerBuilder<string?, byte[]>(config)
    .SetOAuthBearerTokenRefreshHandler(async void (client, _) =>
    {
        try
        {
            var (accessToken, expiresIn) = await FetchUmaToken(tokenEndpoint, clientId, clientSecret);

            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var safeExpiresInMs = Math.Max(1, (expiresIn - 5) * 1000);
            var absoluteExpiry = nowMs + safeExpiresInMs;

            Console.WriteLine($"[AUTH] Refreshing OAuth token (valid {expiresIn}s)");

            client.OAuthBearerSetToken(
                accessToken,
                absoluteExpiry,
                clientId,
                null
            );
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AUTH ERROR] Failed to refresh token: {ex.Message}");
            client.OAuthBearerSetTokenFailure(ex.Message);
        }
    })
    .Build();

consumer.Subscribe(topic);

Console.WriteLine($"Consumer subscribed to topic: {topic}");

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    Console.WriteLine("Shutting down consumer...");
    e.Cancel = true;
    cts.Cancel();
};

try
{
    while (!cts.Token.IsCancellationRequested)
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

return;

static async Task<(string access_token, long expires_in)> FetchUmaToken(
    string tokenEndpoint, string clientId, string clientSecret)
{
    using var http = new HttpClient();

    var body = new Dictionary<string, string>
    {
        ["grant_type"] = "urn:ietf:params:oauth:grant-type:uma-ticket",
        ["client_id"] = clientId,
        ["client_secret"] = clientSecret,
        ["audience"] = "kafka"
    };

    var response = await http.PostAsync(tokenEndpoint, new FormUrlEncodedContent(body));
    var content = await response.Content.ReadAsStringAsync();

    if (!response.IsSuccessStatusCode)
        throw new Exception($"Failed to fetch token: {response.StatusCode} {content}");

    var json = JsonSerializer.Deserialize<JsonElement>(content);

    return (
        json.GetProperty("access_token").GetString()!,
        json.GetProperty("expires_in").GetInt64()
    );
}
