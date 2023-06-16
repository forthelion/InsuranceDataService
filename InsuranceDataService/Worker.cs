using Amazon.Runtime;
using Amazon.SQS.Model;
using Amazon.SQS;
using InsuranceDataService;
using System.Text.Json;
using System.ServiceProcess;
using Amazon;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Xml.Linq;

IHost host = Host.CreateDefaultBuilder(args)
    .UseWindowsService()
    .ConfigureServices(services =>
    {
        services.AddHostedService<Worker>();
    })
    .Build();

await host.RunAsync();

public class Worker : BackgroundService
{
    // you need to change your credtiantianl when running this part
    private const string AwsAccessKey = "ASIAVWS2Z4LH7NHXEGOB";
    private const string AwsSecretKey = "bPTciwaX6tjHnVtdaNrX4ET715X0pk3MubKVhJns";
    private const string AwsSessionToken = "FwoGZXIvYXdzEPz//////////wEaDNsBwmqjV49UntyO1yLRAXGxHmCpFB6uX3udhdGn8X1ouJSUILYlfbKbrAspQB6S4xvGyeW+FcwYJ4kzNEUCiLZuNehb2VKxaDov+Q05Qwp43HsrlO96vaRfwXnAHEn7KqXDPxEbKTurYV6vZiHAzPD27KcNfLqDg8Ss4NVvP+Oz5Nnbno2kXpq4f4+RMxsp74y3GT0YdUfgvzCHi9Mti7/yAjRv22n2Kq+lKw6KYWpDh75gvBjLCMq24zIG9B1oqOi2qKPmKnGOEzOvEx7kLp63WfQdEW8lTfR7/bv06qnsKLbV+KMGMi1y8K4/s780DmYVC/TjghbWZM54Sv069VCQnfdMOeBtcfmwQfPtn8iH/KxVrMs=";

    private readonly ILogger<Worker> _logger;

    // Change here to fit local file mangement 
    //private const string logPath = @"H:\Temp\InsuranceDataService.log";
    private const string logPath = @"C:\Users\Igor\Documents\AWSSERVERFILE\log.txt";
    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await base.StartAsync(cancellationToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            //_logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            //while (true) {
            await pollFromQueue();
            //}
            await Task.Delay(1000, stoppingToken);
            //await uploadToQueue();
        }
    }
    public static void WriteToLog(string message)
    {
        string text = String.Format("{0}:\t{1}", DateTime.Now, message);
        using (StreamWriter writer = new StreamWriter(logPath, append: true))
        {
            writer.WriteLine(text);

        }
    }

    public static async Task pollFromQueue()
    {
        try
        {
            string downQueue = "https://sqs.us-east-1.amazonaws.com/392106205903/downwardQueue";

            AWSCredentials credentials = new SessionAWSCredentials(AwsAccessKey, AwsSecretKey, AwsSessionToken);

            IAmazonSQS client = new AmazonSQSClient(credentials, RegionEndpoint.USEast1);

            var msg = await GetMessage(client, downQueue, 0);

            if (msg.Messages.Count == 0) 
            {
                return;
            }
            string patientIDjson = msg.Messages[0].Body;

            WriteToLog("Read message: " + patientIDjson);



           
            int patientID = ParseJSON(patientIDjson);

            WriteToLog("id is: " + patientID);

            string patientIDString = Convert.ToString(patientID);



            


      
            List<(string patientId, string policyNumber, string provider)> patientIDextracted = xmlXpath();

            if (msg.Messages.Count == 0)
            {
                // No messages in the queue
                // Handle this condition 
                WriteToLog("No messages in the down queue.");
                await Task.Delay(1000);

            }
            else
            {
  
                await DeleteMessage(client, msg.Messages[0], downQueue);

               


                var foundTuple = patientIDextracted.FirstOrDefault(tuple => tuple.patientId == patientIDString);

                if (foundTuple != default)
                {
                    string patientId = foundTuple.patientId;
                    string policyNumber = foundTuple.policyNumber;
                    string provider = foundTuple.provider;
                    Console.WriteLine($"Patient ID: {foundTuple.patientId}");
                    Console.WriteLine($"Policy Number: {foundTuple.policyNumber}");
                    Console.WriteLine($"Provider: {foundTuple.provider}");

                    string message = ("Patient with ID " + patientId + ": policyNumber=" + policyNumber + ", provider=" + provider);
                    //var json = JsonConvert.SerializeObject("{ \"hasInsurance\": \"(Yes)\",  \"insuranceID\": "+patientId+", \"patientID\":\"10001\", \"provider\": \"Liberty Mutual\"} "); 
                    var data = new
                    {
                        id = patientId,
                        policyNumber = policyNumber,
                        provider = provider,
                    };
                    var json = JsonConvert.SerializeObject(data, Newtonsoft.Json.Formatting.Indented);
                    
                    
                    await uploadToQueue(json);
                    WriteToLog("Posted message: " + json);
                }
                else
                {
                    string message = ("Patient with ID "+ patientIDString + " does not  have medical insurance");
                    Console.WriteLine("Patient ID not found");
                    
                    var data = new
                    {
                        id = patientIDString,
                    };
                    var json = JsonConvert.SerializeObject(data, Newtonsoft.Json.Formatting.Indented);
                    await uploadToQueue(json);
                    WriteToLog("Posted message: " + json);
                }


                //patientIDextracted = msg.Messages[0].Body;

                WriteToLog("patientIDjson is: " + patientIDextracted);

        
            }

            

        }
        catch (Exception ex)
        {
            // Handle the exception
            WriteToLog($"Exception occurred: {ex}");
            await Task.Delay(1000);
        }
    }


    public static int ParseJSON(string patientIDjson)
    {
        JObject jsonObject = JObject.Parse(patientIDjson);
        string idValue = (string)jsonObject["id"];
        int parsedId = int.Parse(idValue);
        return parsedId;
    }
    // xml code untested 
    static List<(string patientId, string policyNumber, string provider)> xmlXpath()
    {
        Console.WriteLine("start");
        // change the code here to  to get your xml database
         //string file_path = ;



        // create the list of patientIDds
        List<(string patientId, string policyNumber, string provider)> patientIds = new List<(string patientId, string policyNumber, string provider)>();


        
        XmlDocument xmlDoc = new XmlDocument();

        // change here for local storage 
        xmlDoc.Load("C:\\Users\\Igor\\Downloads\\InsuranceDatabase.xml");
        // errer here 
        XmlNodeList patientNodes = xmlDoc.SelectNodes("/insuranceDatabase/patient");



        // loop each paient to get the data 
        foreach (XmlNode patientNode in patientNodes)
        {

            string patientId = patientNode.Attributes["id"].Value;
            XmlNode policyNode = patientNode.SelectSingleNode("policy");
            string policyNumber = policyNode.Attributes["policyNumber"].Value;
            string provider = policyNode.SelectSingleNode("provider").InnerText;
            patientIds.Add((patientId, policyNumber, provider));



        }

        return patientIds;

    }

   



    

    private static async Task<ReceiveMessageResponse> GetMessage(
        IAmazonSQS sqsClient, string qUrl, int waitTime = 0)
    {
        return await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = qUrl,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = waitTime
            // (Could also request attributes, set visibility timeout, etc.)
        });
    }

    private static async Task DeleteMessage(
        IAmazonSQS sqsClient, Message message, string qUrl)
    {
        WriteToLog($"\nDeleting message {message.MessageId} from queue...");
        await sqsClient.DeleteMessageAsync(qUrl, message.ReceiptHandle);
    }

    public static async Task uploadToQueue(string message)
    {
        WriteToLog("in uploadToQueue");
        // update credtaintional to work 
        string queueUrl = "https://sqs.us-east-1.amazonaws.com/392106205903/UpwardQueue";

        AWSCredentials credentials = new SessionAWSCredentials(AwsAccessKey, AwsSecretKey, AwsSessionToken);

        WriteToLog("creadentials created");

        // Create an Amazon SQS client object using the
        // default user. If the AWS Region you want to use
        // is different, supply the AWS Region as a parameter.
        IAmazonSQS client = new AmazonSQSClient(credentials, RegionEndpoint.USEast1);

        var request = new SendMessageRequest
        {
            MessageBody = message,
            QueueUrl = queueUrl,
        };

        var response = await client.SendMessageAsync(request);

        if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
        {
            WriteToLog($"Successfully sent message. Message ID: {response.MessageId}");
        }
        else
        {
            WriteToLog("Could not send message.");
        }
    }


}

