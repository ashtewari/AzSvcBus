using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using System;
using System.Text;
using Azure.Messaging.ServiceBus;
using System.Configuration;

class Program
{
    const string executionlog = "execution.log";

    static async Task Main(string[] args)
    {
        try
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Missing arguments : fileName");
                return;
            }

            var fileName = args[0];

            string topicName = GetConfigSetting("AsbTopicName");
            string subscriptionName = GetConfigSetting("AsbSubscriptionName");

            string servicebus = GetConnectionStringFromKeyVault("ServiceBusConnectionString");

            await using (ServiceBusClient client = new ServiceBusClient(servicebus))
            {
                await ResendServiceBusMessagesFromFile(client, topicName, fileName);
            }
        }
        catch (Exception exception)
        {
            Console.WriteLine($"{DateTime.Now} :: Exception: {exception.ToString()}");
        }
    }

    private static async Task ResendServiceBusMessagesFromFile(ServiceBusClient client, string topicName, string fileName)
    {
        // Create a sender for the topic
        ServiceBusSender sender = client.CreateSender(topicName);

        var correlationId = System.Guid.NewGuid().ToString();

        string fullPathToFile = System.IO.Path.GetFullPath(fileName);

        if (File.Exists(executionlog))
        {
            var filesAlreadyProcessed = await File.ReadAllLinesAsync(executionlog);
            foreach (var f in filesAlreadyProcessed)
            {
                if (f.ToLowerInvariant() == fullPathToFile.ToLowerInvariant())
                {
                    Console.WriteLine("File already processed. Exiting ..");
                    return;
                }
            }
        }

        Console.WriteLine($"Processing file {fullPathToFile}");

        string tempFile = Path.GetTempFileName();
        System.IO.File.Copy(fullPathToFile, tempFile, true);
        var messages = await File.ReadAllLinesAsync(tempFile);
        System.IO.File.Delete(tempFile);

        AppendTextToFile(executionlog, fullPathToFile);

        await SendMessages(sender, correlationId, messages);
    }

    private static async Task SendMessages(ServiceBusSender sender, string correlationId, string[] messages)
    {
        int count = 0;
        // Send each message to the topic
        foreach (var messageText in messages)
        {
            if (string.IsNullOrWhiteSpace(messageText)) continue;

            count = count + 1;
            ServiceBusMessage message = new ServiceBusMessage(Encoding.UTF8.GetBytes(messageText));
            message.CorrelationId = correlationId;
            message.MessageId = Guid.NewGuid().ToString();
            await sender.SendMessageAsync(message);
            Console.WriteLine($"{DateTime.Now} Sent: {message.MessageId}");

            if (count > 10)
            {
                Console.WriteLine($"{DateTime.Now} Wait ..");
                await Task.Delay(10000);
                count = 1;
            }
        }
    }

    private static string GetConnectionStringFromKeyVault(string key)
    {
        string keyVault = GetConfigSetting("KeyVault");

        // Create an instance of DefaultAzureCredential to authenticate
        // var credential = new DefaultAzureCredential();

        string tenantId = Environment.GetEnvironmentVariable("KVT_TENANT_ID", EnvironmentVariableTarget.User) ?? "";
        string clientId = Environment.GetEnvironmentVariable("KVT_CLIENT_ID", EnvironmentVariableTarget.User) ?? "";
        string clientSecret = Environment.GetEnvironmentVariable("KVT_CLIENT_SECRET", EnvironmentVariableTarget.User) ?? "";

        if (string.IsNullOrEmpty(tenantId) || string.IsNullOrEmpty(clientId) || string.IsNullOrEmpty(clientSecret))
        {
            throw new Exception("Environment variables not set correctly.");
        }

        var credential = new ClientSecretCredential(tenantId, clientId, clientSecret);

        // Create a SecretClient to access the Key Vault
        var secretClient = new SecretClient(new Uri($"https://{keyVault}.vault.azure.net/"), credential);

        try
        {
            // Retrieve a secret from the Key Vault
            KeyVaultSecret secret = secretClient.GetSecret(key);
            return secret.Value;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.ToString()}");
        }

        return string.Empty;
    }

    private static void AppendTextToFile(string file, string message)
    {
        using (StreamWriter writer = File.AppendText(file))
        {
            writer.WriteLine(message);
        }
    }

    private static string GetConfigSetting(string key)
    {
        var setting = ConfigurationManager.AppSettings[key];
        if (setting == null)
        {
            return string.Empty;
        }
        else
        {
            return setting.ToString();
        }
    }
}